from contextlib import asynccontextmanager
from functools import partial

from databases import Database
from fastapi import FastAPI
import strawberry
from strawberry.types import Info
from strawberry.fastapi import BaseContext, GraphQLRouter
from pypika.enums import JoinType
from pypika.dialects import Table, PostgreSQLQuery

from settings import Settings


class Context(BaseContext):
    db: Database

    def __init__(
        self,
        db: Database,
    ) -> None:
        self.db = db


@strawberry.type
class Author:
    name: str


@strawberry.type
class Book:
    title: str
    author: Author


@strawberry.type
class Query:

    @strawberry.field
    async def books(
        self,
        info: Info[Context, None],
        author_ids: list[int] | None = None,
        search: str | None = None,  # Честно говоря, не нашел применение. Вряд ли это offset какой-нибудь
        limit: int | None = None,
    ) -> list[Book]:

        """
        На всякий случай, если требовалось написание raw-sql

        placeholder = '?'
        placeholders = ', '.join(placeholder for _ in author_ids)

        "SELECT b.*, a.name
        FROM books as b
        INNER JOIN authors as a ON b.author_id = a.id
        WHERE a.id IN ({})
        LIMIT {};".format(placeholders, limit)

        info.context.db.execute(str(query), author_ids)

        """

        t_books = Table('books')
        t_authors = Table('authors')

        query = PostgreSQLQuery\
            .from_(t_books)\
            .join(t_authors, JoinType.inner).on(t_books.author_id == t_authors.id) \
            .select(t_books.star).select(t_authors.name)

        if author_ids:
            query = query.where(t_authors.field('id').isin(author_ids))

        if limit:
            query = query.limit(limit)

        return [
            Book(title=record._mapping['title'], author=Author(name=record._mapping['name']))
            for record
            in await info.context.db.fetch_all(query=str(query))
        ]


CONN_TEMPLATE = "postgresql+asyncpg://{user}:{password}@{host}:{port}/{name}"
db = Database(
    CONN_TEMPLATE.format(
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        port=settings.DB_PORT,
        host=settings.DB_SERVER,
        name=settings.DB_NAME,
    ),
)


@asynccontextmanager
async def lifespan(
    app: FastAPI,
    db: Database,
):
    async with db:
        yield

schema = strawberry.Schema(query=Query)
graphql_app = GraphQLRouter(  # type: ignore
    schema,
    context_getter=partial(Context, db),
)

app = FastAPI(lifespan=partial(lifespan, db=db))
app.include_router(graphql_app, prefix="/graphql")
