import type { ZodType } from 'zod'

import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import type { Logger } from '@crowd/logging'

import type { EmitOverrides } from './emit'
import type { ConnectorHttp, ResponseInterpreter } from './http/client'
import type { BudgetProbe, TokenMinter, TokenPool } from './pool/tokenPool'

export interface Channel {
  channelId: string
  channelName: string
}

export interface GithubAppCredentialData {
  appId: string
  privateKey: string
}

export interface OctolensTokenCredentialData {
  token: string
}

export interface CredlyOAuthClientCredentialData {
  clientId: string
  clientSecret: string
  organizationId: string
}

export type Credential =
  | { platform: 'github'; kind: 'github-app'; data: GithubAppCredentialData }
  | { platform: 'octolens'; kind: 'octolens-token'; data: OctolensTokenCredentialData }
  | { platform: 'credly'; kind: 'credly-oauth-client'; data: CredlyOAuthClientCredentialData }

export function assertCredentialKind<K extends Credential['kind']>(
  credential: Credential,
  kind: K,
): asserts credential is Extract<Credential, { kind: K }> {
  if (credential.kind !== kind) {
    throw new Error(`expected ${kind} credential, got ${credential.kind}`)
  }
}

export interface SyncContext<TDeps = undefined> {
  channel: Channel
  integrationId: string
  watermark: Record<string, unknown> | null
  // False while the unit runs in shadow mode: emit writes shadow records, nothing reaches the sink.
  emitEnabled?: boolean
  emit: (records: unknown[], overrides?: EmitOverrides) => Promise<void>
  commitWatermark: (watermark: Record<string, unknown>) => Promise<void>
  hasRunBudget: () => boolean
  http: ConnectorHttp
  log: Logger
  // Connectors without preparePool/mintToken (e.g. Octolens) get no credential via ctx.http's
  // token pool, so they fetch their own here instead.
  getCredential: () => Promise<Credential>
  deps: TDeps
}

export interface SyncOutcome {
  complete: boolean
  // Only meaningful with complete: false; the next run then waits for the sync's normal cadence.
  resumeAtCadence?: boolean
}

export interface SyncDefinition<TDeps = undefined> {
  name: string
  cadenceMinutes: number
  schema: ZodType<Record<string, unknown>>
  run: (ctx: SyncContext<TDeps>) => Promise<SyncOutcome>
}

export interface PoolPreparation {
  preferredEntryId?: string
}

export interface DepsFactoryInput {
  qx: QueryExecutor
  integrationId: string
}

export type DepsFactory<TDeps> = (input: DepsFactoryInput) => TDeps

interface ManifestBase<TDeps> {
  platform: string
  syncs: SyncDefinition<TDeps>[]
  discover: (credential: Credential) => Promise<Channel[]>
  preparePool?: (credential: Credential, pool: TokenPool) => Promise<PoolPreparation>
  mintToken?: (credential: Credential) => TokenMinter
  probeBudget?: BudgetProbe
  interpretResponse?: ResponseInterpreter
  // Every emit of every sync passes both segmentId and integrationId overrides, so the run may
  // belong to an integration with no segment (e.g. one org-wide poller fanning out to subprojects).
  emitsOwnSegment?: boolean
}

// A connector whose syncs need deps must provide the factory that builds them. Tuple-wrapped
// check, since services compile without strictNullChecks (undefined extends everything there).
export type Manifest<TDeps = undefined> = ManifestBase<TDeps> &
  ([TDeps] extends [undefined]
    ? { createDeps?: DepsFactory<TDeps> }
    : { createDeps: DepsFactory<TDeps> })

export type AnyManifest = ManifestBase<unknown> & { createDeps?: DepsFactory<unknown> }
