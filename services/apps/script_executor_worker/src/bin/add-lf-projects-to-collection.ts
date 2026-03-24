/**
 * Add Linux Foundation Projects to Collection Script
 *
 * PROBLEM:
 * Linux Foundation projects (isLf = true) need to be added to a specific collection
 * in the collectionsInsightsProjects table for proper organization.
 *
 * SOLUTION:
 * This script finds all Linux Foundation projects from insightsProject table
 * and adds them to the specified collection in collectionsInsightsProjects table.
 *
 * Usage:
 *   # Via package.json script (recommended):
 *   pnpm run add-lf-projects-to-collection -- [--batch-size <size>] [--dry-run]
 *
 *   # Or directly with tsx:
 *   npx tsx src/bin/add-lf-projects-to-collection.ts [--batch-size <size>] [--dry-run]
 *
 * Options:
 *   --batch-size       Number of records to process in each batch (default: 1000)
 *   --dry-run          Display what would be added without actually adding anything
 *
 * Environment Variables Required:
 *   CROWD_DB_WRITE_HOST - Postgres write host
 *   CROWD_DB_PORT - Postgres port
 *   CROWD_DB_USERNAME - Postgres username
 *   CROWD_DB_PASSWORD - Postgres password
 *   CROWD_DB_DATABASE - Postgres database name
 */
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { QueryExecutor, pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

const log = getServiceChildLogger('add-lf-projects-to-collection-script')

// Constants
// Collection ID related to Linux Foundation projects
const COLLECTION_ID = '1606ad11-c96d-4177-8147-8f990b76b35d'
const DEFAULT_STARRED = false

// Type definitions
interface LfProject {
  id: string
  name: string
}

interface BatchResult {
  addedCount: number
  success: boolean
  error?: string
}

interface ProcessSummary {
  totalBatches: number
  totalAdded: number
  failedBatches: number
  startTime: string
  endTime: string
  batchSize: number
  dryRun: boolean
}

/**
 * Initialize Postgres connection using QueryExecutor
 */
async function initPostgresClient(): Promise<QueryExecutor> {
  log.info('Initializing Postgres connection...')

  const dbConnection = await getDbConnection(WRITE_DB_CONFIG())
  const queryExecutor = pgpQx(dbConnection)

  log.info('Postgres connection established')
  return queryExecutor
}

/**
 * Get Linux Foundation projects that are not deleted and not already in the collection
 */
async function getLfProjectsNotInCollection(
  postgres: QueryExecutor,
  batchSize: number,
  offset: number,
): Promise<LfProject[]> {
  // Since result() returns row count, not data, we need to iterate row by row
  // or use a different approach. Let's get them one by one for now.

  const projects: LfProject[] = []

  for (let i = 0; i < batchSize; i++) {
    const singleOffset = offset + i

    const query = `
      SELECT ip.id, ip.name
      FROM public."insightsProjects" ip
      LEFT JOIN public."collectionsInsightsProjects" cip 
        ON ip.id = cip."insightsProjectId" 
        AND cip."collectionId" = $(collectionId)
      WHERE ip."isLF" = true 
        AND ip."deletedAt" IS NULL
        AND cip."insightsProjectId" IS NULL
      ORDER BY ip.id
      LIMIT 1 OFFSET $(singleOffset)
    `

    try {
      const result = await postgres.selectOneOrNone(query, {
        collectionId: COLLECTION_ID,
        singleOffset,
      })

      if (result) {
        projects.push(result as LfProject)
      } else {
        // No more results, break the loop
        break
      }
    } catch (error) {
      log.error(`Error fetching project at offset ${singleOffset}: ${error.message}`)
      break
    }
  }

  log.info(`Fetched ${projects.length} project(s) in this batch`)

  return projects
}

/**
 * Count total Linux Foundation projects not in collection
 */
async function countLfProjectsNotInCollection(postgres: QueryExecutor): Promise<number> {
  log.info('Counting Linux Foundation projects not in collection...')

  const query = `
    SELECT COUNT(*)::int as count
    FROM public."insightsProjects" ip
    LEFT JOIN public."collectionsInsightsProjects" cip 
      ON ip.id = cip."insightsProjectId" 
      AND cip."collectionId" = $(collectionId)
    WHERE ip."isLF" = true 
      AND ip."deletedAt" IS NULL
      AND cip."insightsProjectId" IS NULL
  `

  const result = await postgres.selectOne(query, { collectionId: COLLECTION_ID })
  const count = (result as { count: number }).count

  log.info(`Found ${count} Linux Foundation project(s) not in collection`)
  return count
}

/**
 * Add one batch of LF projects to collection
 */
async function addLfProjectsToCollectionBatch(
  postgres: QueryExecutor,
  projects: LfProject[],
  dryRun: boolean,
): Promise<BatchResult> {
  if (projects.length === 0) {
    return {
      addedCount: 0,
      success: true,
    }
  }

  try {
    if (dryRun) {
      log.info(`[DRY RUN] Would add ${projects.length} project(s) to collection`)
      projects.forEach((project) => {
        log.info(`[DRY RUN] Would add project: ${project.name} (${project.id})`)
      })
      return {
        addedCount: projects.length,
        success: true,
      }
    }

    // Insert projects one by one since batch insert isn't working with our QueryExecutor
    let addedCount = 0

    for (const project of projects) {
      try {
        const query = `
          INSERT INTO public."collectionsInsightsProjects" 
            ("collectionId", "insightsProjectId", "starred")
          VALUES ($(collectionId), $(insightsProjectId), $(starred))
        `

        await postgres.result(query, {
          collectionId: COLLECTION_ID,
          insightsProjectId: project.id,
          starred: DEFAULT_STARRED,
        })

        addedCount++
        log.info(`  - Added: ${project.name} (${project.id})`)
      } catch (error) {
        log.error(`Failed to add project ${project.name} (${project.id}): ${error.message}`)
      }
    }

    log.info(`✓ Added ${addedCount} out of ${projects.length} project(s) to collection`)

    return {
      addedCount,
      success: true,
    }
  } catch (error) {
    log.error(`Failed to add projects to collection: ${error.message}`)
    return {
      addedCount: 0,
      success: false,
      error: error.message,
    }
  }
}

/**
 * Process adding projects to collection in batches
 */
async function processAddition(
  postgres: QueryExecutor,
  batchSize: number,
  dryRun: boolean,
): Promise<ProcessSummary> {
  const startTime = new Date().toISOString()
  let totalBatches = 0
  let totalAdded = 0
  let failedBatches = 0
  let offset = 0

  log.info(`Starting addition process with batch size: ${batchSize}`)

  if (dryRun) {
    log.info('🧪 DRY RUN MODE - No data will be added')
  }

  // Count total projects to add
  const totalToAdd = await countLfProjectsNotInCollection(postgres)

  if (totalToAdd === 0) {
    log.info('No Linux Foundation projects need to be added to collection. Process complete.')

    return {
      totalBatches: 0,
      totalAdded: 0,
      failedBatches: 0,
      startTime,
      endTime: new Date().toISOString(),
      batchSize,
      dryRun,
    }
  }

  log.info(`Processing ${totalToAdd} Linux Foundation project(s) in batches of ${batchSize}`)

  let hasMore = true

  while (hasMore) {
    totalBatches++

    log.info(`Processing batch ${totalBatches} (offset: ${offset})...`)

    // Get batch of projects
    const projects = await getLfProjectsNotInCollection(postgres, batchSize, offset)

    if (projects.length === 0) {
      hasMore = false
      log.info('No more projects to process.')
      break
    }

    // Add projects to collection
    const batchResult = await addLfProjectsToCollectionBatch(postgres, projects, dryRun)

    if (batchResult.success) {
      totalAdded += batchResult.addedCount

      // If we processed fewer projects than the batch size, we're done
      if (projects.length < batchSize) {
        hasMore = false
        log.info(
          `Batch ${totalBatches} processed ${projects.length} project(s). No more projects to process.`,
        )
      } else {
        log.info(`Batch ${totalBatches} completed. ${projects.length} project(s) processed.`)
        // Don't increment offset! After successful insertions, the filtered result set shrinks
        // so we should query from the same offset to get the next batch of unprocessed projects
        if (dryRun) {
          // In dry-run mode, no insertions occur, so we need to increment offset to avoid infinite loop
          offset += batchSize
        }
        // In live mode, keep offset the same since inserted projects are now filtered out
      }
    } else {
      failedBatches++
      log.error(`Batch ${totalBatches} failed: ${batchResult.error}`)
      // On failure, increment offset to move past problematic records
      offset += batchSize
    }

    // Safety check to prevent infinite loops
    if (totalBatches >= 1000) {
      log.warn('Reached maximum batch limit (1000). Stopping to prevent infinite loops.')
      hasMore = false
    }
  }

  const endTime = new Date().toISOString()

  return {
    totalBatches,
    totalAdded,
    failedBatches,
    startTime,
    endTime,
    batchSize,
    dryRun,
  }
}

/**
 * Main entry point
 */
async function main() {
  const args = process.argv.slice(2)

  // Parse command line arguments
  const batchSizeIndex = args.indexOf('--batch-size')
  const dryRunIndex = args.indexOf('--dry-run')

  const dryRun = dryRunIndex !== -1

  let batchSize = 1000 // Default batch size
  if (batchSizeIndex !== -1) {
    if (batchSizeIndex + 1 >= args.length) {
      log.error('Error: --batch-size requires a value')
      process.exit(1)
    }
    const batchSizeValue = parseInt(args[batchSizeIndex + 1], 10)
    if (isNaN(batchSizeValue) || batchSizeValue <= 0) {
      log.error('Error: --batch-size must be a positive integer')
      process.exit(1)
    }
    batchSize = batchSizeValue
  }

  // Check for help flag or invalid arguments
  if (args.includes('--help') || args.includes('-h')) {
    log.info(`
      Usage:
        # Via package.json script (recommended):
        pnpm run add-lf-projects-to-collection -- [--batch-size <size>] [--dry-run]
        
        # Or directly with tsx:
        npx tsx src/bin/add-lf-projects-to-collection.ts [--batch-size <size>] [--dry-run]
      
      Options:
        --batch-size <size>: Number of records to process in each batch (default: 1000)
        --dry-run: Display what would be added without actually adding anything
      
      Examples:
        # Run addition with default batch size (1000)
        pnpm run add-lf-projects-to-collection
        
        # Run with custom batch size
        pnpm run add-lf-projects-to-collection -- --batch-size 500
        
        # Dry run to preview what would be added
        pnpm run add-lf-projects-to-collection -- --dry-run
        
        # Dry run with custom batch size
        pnpm run add-lf-projects-to-collection -- --batch-size 100 --dry-run
      
      Collection ID: ${COLLECTION_ID}
      Default starred value: ${DEFAULT_STARRED}
    `)
    process.exit(0)
  }

  try {
    log.info(`\n${'='.repeat(80)}`)
    log.info('Add Linux Foundation Projects to Collection Script')
    log.info(`${'='.repeat(80)}`)
    log.info(`Collection ID: ${COLLECTION_ID}`)
    log.info(`Batch size: ${batchSize}`)
    log.info(`Default starred: ${DEFAULT_STARRED}`)
    if (dryRun) {
      log.info('Mode: DRY RUN (no data will be added)')
    } else {
      log.info('Mode: LIVE (data will be added)')
    }
    log.info(`${'='.repeat(80)}\n`)

    // Initialize database connection
    const postgres = await initPostgresClient()

    // Process addition
    const summary = await processAddition(postgres, batchSize, dryRun)

    // Print final summary
    log.info(`\n${'='.repeat(80)}`)
    log.info('Addition Summary')
    log.info(`${'='.repeat(80)}`)
    log.info(`✓ Total batches processed: ${summary.totalBatches}`)
    log.info(`✓ Total projects ${dryRun ? 'found' : 'added'}: ${summary.totalAdded}`)
    if (summary.failedBatches > 0) {
      log.warn(`✗ Failed batches: ${summary.failedBatches}`)
    }
    log.info(`Duration: ${summary.startTime} → ${summary.endTime}`)

    const exitCode = summary.failedBatches > 0 ? 1 : 0
    process.exit(exitCode)
  } catch (error) {
    log.error(error, 'Failed to run addition script')
    log.error(`\n❌ Error: ${error.message}`)
    process.exit(1)
  }
}

main().catch((error) => {
  log.error('Unexpected error:', error)
  process.exit(1)
})
