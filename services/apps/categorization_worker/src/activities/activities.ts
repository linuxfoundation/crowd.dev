import { LlmService } from '@crowd/common_services'
import { dbStoreQx } from '@crowd/data-access-layer'
import { listCategories } from '@crowd/data-access-layer/src/categories'
import {
  CollectionField,
  ICollection,
  InsightsProjectField,
  connectProjectsAndCollections,
  queryCollections,
  queryInsightsProjects,
} from '@crowd/data-access-layer/src/collections'

import { svc } from '../main'
import { IFindCategoryParams, IFindCollectionsParams, IListedCategory } from '../types'

function validateAndCorrectLLMItems<T extends { name: string; id: string }>(
  llmItems: T[],
  databaseItems: T[],
  itemType: string,
): T[] {
  if (!llmItems || llmItems.length === 0) {
    return []
  }

  const validUuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

  return llmItems
    .map((llmItem) => {
      // Try to find by ID if UUID is valid
      if (validUuidRegex.test(llmItem.id)) {
        const dbItem = databaseItems.find((item) => item.id === llmItem.id)
        if (dbItem) {
          return { name: dbItem.name, id: dbItem.id } as T
        }
        svc.log.warn(`${itemType} UUID "${llmItem.id}" not found in database, trying name lookup`)
      } else {
        svc.log.warn(
          `${itemType} has invalid UUID format: "${llmItem.id}" (length: ${llmItem.id?.length || 0}), trying name lookup`,
        )
      }

      // Fallback: try to find by name
      const dbItem = llmItem.name
        ? databaseItems.find((item) => item.name.toLowerCase() === llmItem.name.toLowerCase())
        : null

      if (dbItem) {
        svc.log.info(`Found ${itemType} "${llmItem.name}" by name, using DB UUID "${dbItem.id}"`)
        return { name: dbItem.name, id: dbItem.id } as T
      }

      svc.log.warn(`${itemType} "${llmItem.name}" not found in database, skipping`)
      return null
    })
    .filter(Boolean)
}

function formatTextCategoriesForPrompt(categories: IListedCategory[]): string {
  const categoryObjects = categories.map((category) => ({
    name: category.name,
    id: category.id,
    categoryGroupName: category.categoryGroupName,
  }))

  return JSON.stringify(categoryObjects, null, 2)
}

function formatTextCollectionsForPrompt(
  collections: ICollection[],
  categories: Partial<IListedCategory>[],
): string {
  if (collections.length === 0 || categories.length === 0) return ''

  const categoryMap = new Map<string, string>(categories.map((cat) => [cat.id, cat.name]))

  const groupedCollections: Record<string, ICollection[]> = {}

  for (const collection of collections) {
    const categoryName = categoryMap.get(collection.categoryId)

    if (!groupedCollections[categoryName]) {
      groupedCollections[categoryName] = []
    }

    groupedCollections[categoryName].push({
      ...collection,
      description: collection.description,
    })
  }

  let collectionsText = ''

  for (const categoryName of Object.keys(groupedCollections)) {
    collectionsText += `### ${categoryName}\n`
    for (const collection of groupedCollections[categoryName]) {
      collectionsText += `- **${collection.name}**: ${collection.description}, id: ${collection.id}\n`
    }
    collectionsText += '\n'
  }

  return collectionsText.trim()
}

export async function findCategoriesWithLLM({
  description,
  github,
  topics,
  website,
}: IFindCategoryParams) {
  const qx = dbStoreQx(svc.postgres.writer)

  const categories = await listCategories(qx, {
    query: '',
    limit: 1000,
    offset: 0,
    groupType: null,
  })

  const prompt = `
    You are an expert open-source analyst. Your job is to classify ${github} into appropriate categories.

    ## Context and Purpose
    This classification is part of the Open Source Index, a comprehensive catalog of the most critical open-source projects.
    Developers and organizations use this index to:
    - Discover relevant open-source tools for their technology stack
    - Understand the open-source ecosystem in their domain
    - Make informed decisions about which projects to adopt or contribute to
    - Assess the health and importance of projects in specific technology areas

    Accurate categorization is essential for users to find the right projects when browsing by technology domain or industry vertical.

    ## Project Information
    - URL: ${github}
    - Description: ${description}
    - Topics: ${topics}
    - Homepage: ${website}

    ## Available Categories (AUTHORITATIVE, CLOSED SET)
    The following categories are the ONLY valid options.
    They are provided as a JSON array of immutable objects.
    Every valid category is exactly one object in this array:

    ${formatTextCategoriesForPrompt(categories)}

    ## NON-NEGOTIABLE OUTPUT CONSTRAINTS (MUST FOLLOW)
    - You MUST select categories ONLY from the JSON array above.
    - You MUST NOT invent categories.
    - You MUST NOT generate new ids.
    - You MUST NOT retype, rephrase, normalize, translate, or modify ANY character
      of any selected category object's "name" or "id".
    - The output "categories" MUST be a subset of objects copied EXACTLY from the array above.
    - If you cannot comply perfectly, return {"categories": []}.

    ### MANDATORY SELF-CHECK BEFORE FINAL OUTPUT
    For each object you plan to output in "categories":
    1) Confirm there is an IDENTICAL object in the provided JSON array (same "name" string, same "id" string).
    2) If not identical, REMOVE it (do not replace it).

    ## Your Task
    Analyze the project and determine which categories it belongs to.
    A project can belong to multiple categories if appropriate.

    Consider:
    - The project's primary functionality and purpose
    - The technology domain it operates in
    - The industry or vertical it serves (if applicable)
    - How developers would expect to find this project when browsing by category

    If the project doesn't clearly fit into any of the available categories, return an empty array for categories.

    ## Format
    Respond with a valid JSON object ONLY.
    Do not include explanations outside the JSON.
    Do not include markdown formatting or extra text.

    If the project fits one or more categories:
    {
      "categories": [
      { "name": "Source Code Management", "id": "9a66d814-22b8-493d-a3a7-fb2d9e93587c" }
      ],
      "explanation": "Brief explanation of why you chose these categories"
    }

    If the project does not clearly fit any category OR if any mismatch risk exists:
    {
      "categories": []
    }
  `

  const llmService = new LlmService(
    qx,
    {
      accessKeyId: process.env.CROWD_AWS_BEDROCK_ACCESS_KEY_ID,
      secretAccessKey: process.env.CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY,
    },
    svc.log,
  )

  const { result } = await llmService.findRepoCategories<{
    categories: { name: string; id: string }[]
    explanation: string
  }>(prompt)

  // Check if result is null (LLM disabled or error)
  if (!result) {
    svc.log.warn('LLM service returned null result, skipping categorization')
    return { categories: [], explanation: 'LLM service unavailable' }
  }

  // Validate and correct UUIDs from LLM response
  if (Array.isArray(result.categories) && result.categories.length > 0) {
    result.categories = validateAndCorrectLLMItems(result.categories, categories, 'Category')
  } else if (result.categories && !Array.isArray(result.categories)) {
    svc.log.error(`LLM returned categories as non-array: ${typeof result.categories}`)
    result.categories = []
  }

  svc.log.info(`categories found: ${JSON.stringify(result)}`)

  return result
}

export async function findCollectionsWithLLM({
  categories,
  description,
  github,
  topics,
  website,
}: IFindCollectionsParams) {
  const qx = dbStoreQx(svc.postgres.writer)

  if (!categories || categories.length === 0) {
    return null
  }

  const collections = await queryCollections(qx, {
    fields: Object.values(CollectionField),
    filter: {
      categoryId: { in: categories.map((c) => c.id) },
    },
    orderBy: '"name" ASC',
  })

  const prompt = `
    You are an expert open-source analyst. Your job is to classify ${github} into appropriate collections.

    ## Context and Purpose
    This classification is part of the Open Source Index, a comprehensive catalog of the most critical open-source projects. 
    Developers and organizations use this index to:
    - Discover relevant open-source tools for their technology stack
    - Understand the open-source ecosystem in their domain
    - Make informed decisions about which projects to adopt or contribute to
    - Assess the health and importance of projects in specific technology areas

    Accurate classification is essential for users to find the right projects when browsing by technology domain or industry vertical.

    ## Project Information
    - URL: ${github}
    - Description: ${description}
    - Topics: ${topics}
    - Homepage: ${website}

    ## Categories
    The project has been identified as belonging to these categories:
    ${categories.join(', ')}

    ## Available Collections
    These are the collections within those categories:

    ${formatTextCollectionsForPrompt(collections, categories)}

    ## Your Task
    Analyze the project carefully and assign it to all relevant collections. 
    A project can (and often does) belong to **multiple collections** if it serves multiple use cases, domains, or technical functions.
    Do **not** limit the classification to a single collection unless it clearly fits only one.
    Use your judgment to consider the primary and secondary purposes of the project.

    Consider:
    - The specific functionality the project provides
    - How it compares to other projects in potential collections
    - Whether it's a horizontal tool (used across industries) or vertical solution (industry-specific)
    - The granularity of the collections (neither too broad nor too specific)

    If the project doesn't clearly fit into any of the available collections, return an empty list for collections, and **do not include an explanation**.

    ## Format
    Return **only** a valid JSON object in the following format — without any explanations, markdown, or introductory text:

    If one or more collections apply:
    {
      "collections": [
        { "name": "collectionName", "id": "CollectionID" },
        { "name": "anotherCollectionName", "id": "AnotherID" }
      ],
      "explanation": "Brief explanation of why you chose these collections."
    }

    If no collections apply:
    {
      "collections": []
    }

    ### Rules:
    - Do **not** include any markdown formatting or code block syntax (like triple backticks).
    - Do **not** include the word "json" or any introductory/explanatory text.
    - The output must be **only** a valid JSON object.
    - Use **only** the collections provided — do not invent new ones.
    `

  const llmService = new LlmService(
    qx,
    {
      accessKeyId: process.env.CROWD_AWS_BEDROCK_ACCESS_KEY_ID,
      secretAccessKey: process.env.CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY,
    },
    svc.log,
  )

  const { result } = await llmService.findRepoCollections<{
    collections: { name: string; id: string }[]
    explanation: string
  }>(prompt)

  // Check if result is null (LLM disabled or error)
  if (!result) {
    svc.log.warn('LLM service returned null result, skipping collection classification')
    return { collections: [], explanation: 'LLM service unavailable' }
  }

  // Validate and correct UUIDs from LLM response
  if (Array.isArray(result.collections) && result.collections.length > 0) {
    const validatedCollections = validateAndCorrectLLMItems(
      result.collections,
      collections,
      'Collection',
    )
    result.collections = validatedCollections

    // Log the validated collection IDs for debugging
    svc.log.info(
      `Validated collections: ${validatedCollections.map((c) => `${c.name}:${c.id}`).join(', ')}`,
    )
  } else if (result.collections && !Array.isArray(result.collections)) {
    svc.log.error(`LLM returned collections as non-array: ${typeof result.collections}`)
    result.collections = []
  }

  svc.log.info(`collections found: ${JSON.stringify(result)}`)

  return result
}

export async function findInsightsProjectBySegmentId(segmentId: string) {
  const qx = dbStoreQx(svc.postgres.writer)

  const result = await queryInsightsProjects(qx, {
    filter: {
      segmentId: { eq: segmentId },
    },
    fields: Object.values(InsightsProjectField),
  })

  return result
}

export async function connectProjectAndCollection(
  collectionIds: string[],
  insightsProjectId: string,
) {
  if (collectionIds.length === 0) {
    svc.log.warn(`No collection IDs to connect for project ${insightsProjectId}, skipping`)
    return
  }

  svc.log.info(`updating the collections: ${collectionIds} with the project: ${insightsProjectId}`)
  await connectProjectsAndCollections(
    dbStoreQx(svc.postgres.writer),
    collectionIds.map((collectionId) => ({
      insightsProjectId,
      collectionId,
      starred: false,
    })),
    'DO NOTHING',
  )
}
