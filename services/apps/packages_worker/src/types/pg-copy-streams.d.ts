// Local type declarations for pg-copy-streams v7.
//
// The DefinitelyTyped package (@types/pg-copy-streams) is abandoned at v1.2.5 and never tracked
// the v7 API: it advertises a stale major and lacks `both`. v7's runtime shape is identical for
// `from`/`to` (Writable/Readable streams carrying `text`/`rowCount`) and adds `both` (a Duplex).
// We own the declaration here instead of depending on a mismatched @types major.
//
// Kept self-contained (no `import ... from 'pg'`): the streams are pg "Submittable" objects — node-pg
// calls submit(connection) internally — but our code never references pg's Submittable/Connection
// types directly (loadDump.ts only reads `rowCount` and pipes the stream), so we avoid coupling this
// package to @types/pg. `submit` is typed loosely for the same reason.
declare module 'pg-copy-streams' {
  import { Duplex, Readable, ReadableOptions, Writable, WritableOptions } from 'stream'

  export class CopyStreamQuery extends Writable {
    text: string
    rowCount: number
    submit(connection: unknown): void
  }

  export class CopyToStreamQuery extends Readable {
    text: string
    rowCount: number
    submit(connection: unknown): void
  }

  export class CopyBothQueryStream extends Duplex {
    text: string
    rowCount: number
    submit(connection: unknown): void
  }

  export function from(txt: string, options?: WritableOptions): CopyStreamQuery
  export function to(txt: string, options?: ReadableOptions): CopyToStreamQuery
  export function both(
    txt: string,
    options?: ReadableOptions & WritableOptions,
  ): CopyBothQueryStream
}
