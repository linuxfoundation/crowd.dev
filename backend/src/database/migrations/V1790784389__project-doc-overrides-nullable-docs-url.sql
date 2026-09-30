-- An active override with a NULL docsUrl means "this project has no docs" (IN-1423).
ALTER TABLE public."projectDocOverrides"
    ALTER COLUMN "docsUrl" DROP NOT NULL;
