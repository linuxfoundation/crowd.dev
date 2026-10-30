import { z } from 'zod'

export const CREDLY_BADGE_STATES = [
  'pending',
  'accepted',
  'rejected',
  'revoked',
  'replaced',
] as const

export type CredlyBadgeState = (typeof CREDLY_BADGE_STATES)[number]

const nullableString = z.string().nullable().optional()

const credlyUserSchema = z
  .object({
    id: z.string().optional(),
    url: nullableString,
    photo_url: nullableString,
    confirmed: z.boolean().optional(),
  })
  .passthrough()

const credlyBadgeTemplateSchema = z
  .object({
    id: z.string(),
    name: z.string(),
    url: nullableString,
    vanity_slug: nullableString,
    state: nullableString,
    type_category: nullableString,
    level: nullableString,
    skills: z.array(z.string()).nullable().optional(),
    global_activity_url: nullableString,
  })
  .passthrough()

export const credlyBadgeSchema = z
  .object({
    id: z.string(),
    state: z.enum(CREDLY_BADGE_STATES),
    issued_at: z.string(),
    created_at: nullableString,
    expires_at: nullableString,
    state_updated_at: nullableString,
    recipient_email: z.string().trim().min(1),
    issued_to: nullableString,
    issuer_earner_id: nullableString,
    badge_url: nullableString,
    image_url: nullableString,
    public: z.boolean().optional(),
    revocation_reason: nullableString,
    replacement_badge_id: nullableString,
    replaces_badge_id: nullableString,
    user: credlyUserSchema.nullable().optional(),
    badge_template: credlyBadgeTemplateSchema,
  })
  .passthrough()

export type CredlyBadge = z.infer<typeof credlyBadgeSchema>
