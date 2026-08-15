"use strict";
/* eslint-disable no-console */
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/* eslint-disable no-continue */
/**
 * TBD
 */
const command_line_args_1 = __importDefault(require("command-line-args"));
const command_line_usage_1 = __importDefault(require("command-line-usage"));
const fs = __importStar(require("fs"));
const path_1 = __importDefault(require("path"));
const databaseConnection_1 = require("@/database/databaseConnection");
const sequelizeRepository_1 = __importDefault(require("@/database/repositories/sequelizeRepository"));
const options = [
    {
        name: 'help',
        alias: 'h',
        type: Boolean,
        description: 'Print this usage guide.',
    },
    {
        name: 'file',
        alias: 'f',
        type: String,
        description: 'Path to JSON file to consolidate projects from',
    },
    {
        name: 'dryRun',
        alias: 'd',
        type: Boolean,
        description: 'Dry run mode. Will not delete any projects. Will print the projects to be deleted.',
    },
];
const sections = [
    {
        header: 'Consolidate Insights Projects',
        content: 'Consolidates insights projects based on the main repository URL from the CSV file.',
    },
    {
        header: 'Options',
        optionList: options,
    },
];
/**
 * Parses a JSON file containing project information.
 * @param filePath - The path to the JSON file to parse
 * @returns An array of project objects containing project information from the JSON
 */
function parseJSON(filePath) {
    const fileData = fs.readFileSync(path_1.default.resolve(filePath), 'utf-8');
    return JSON.parse(fileData);
}
async function cleanUpDuplicateProjects(qx, internalProjects, dryRun) {
    let matchedCount = 0;
    let deletedCount = 0;
    // Check for segmentId in related projects
    for (const project of internalProjects) {
        const projectToDelete = await qx.result(`SELECT * FROM "insightsProjects" 
                WHERE "github" = $1
                AND "segmentId" IS NULL
                AND "isLF" = false`, [project]);
        if (projectToDelete.rows.length > 0) {
            matchedCount++;
            console.log(`Project ${projectToDelete.rows[0].name} match`);
        }
        else {
            console.log(`No match for ${project}`);
            continue;
        }
        if (!dryRun) {
            const replacementProject = await qx.result(`SELECT * 
                FROM "insightsProjects" ip 
                WHERE ip.id != $1 
                AND $2 = ANY(ip."repositories")
                LIMIT 1`, [projectToDelete.rows[0].id, project]);
            if (replacementProject.rows.length > 0) {
                const updatedLinks = await qx.result(`
                    UPDATE "collectionsInsightsProjects" cip
                    SET
                        "insightsProjectId" = $1,
                        "updatedAt" = NOW()
                    WHERE "insightsProjectId" = $2
                    AND NOT EXISTS (
                        SELECT 1 
                        FROM "collectionsInsightsProjects"
                        WHERE "collectionId" = cip."collectionId"
                        AND "insightsProjectId" = $1
                    )
                    RETURNING *
                    `, [replacementProject.rows[0].id, projectToDelete.rows[0].id]);
                if (updatedLinks.rows.length > 0) {
                    console.log(`Updated collection insights project to point to replacement project ${replacementProject.rows[0].id}`);
                }
                else {
                    console.log(`Skipping to update links for ${projectToDelete.rows[0].name} project`);
                }
                const deletedLinks = await qx.result(`UPDATE "collectionsInsightsProjects" 
                    SET "deletedAt" = NOW()
                    WHERE "insightsProjectId" = $1 AND "deletedAt" IS NULL
                    RETURNING *`, [projectToDelete.rows[0].id]);
                if (deletedLinks.rows.length > 0) {
                    console.log(`Deleted ${deletedLinks.rows.length} collection insights project links`);
                }
                else {
                    console.log(`Skipping to delete links for ${projectToDelete.rows[0].name} project`);
                }
                await qx.result(`DELETE FROM "insightsProjects" 
                        WHERE id = $1`, [projectToDelete.rows[0].id]);
                deletedCount++;
                console.log(`Deleted ${projectToDelete.rows[0].name} project`);
            }
            else {
                console.log(`Skipping ${projectToDelete.rows[0].name} project because no replacement project found`);
            }
        }
    }
    console.log(`\nSummary:`);
    console.log(`- Found ${matchedCount} matching projects`);
    if (!dryRun) {
        console.log(`- Deleted ${deletedCount} projects`);
    }
    else {
        console.log(`- Would delete ${matchedCount} projects (dry run)`);
    }
}
const usage = (0, command_line_usage_1.default)(sections);
const parameters = (0, command_line_args_1.default)(options);
if (parameters.help || !parameters.file) {
    console.log(usage);
}
else {
    setImmediate(async () => {
        try {
            const prodDb = await (0, databaseConnection_1.databaseInit)();
            const qx = sequelizeRepository_1.default.getQueryExecutor({
                database: prodDb,
            });
            // Parse JSON file
            const projects = parseJSON(parameters.file);
            const parsedProjects = Object.keys(projects)
                .filter((project) => projects[project].internal)
                .map((project) => `https://github.com/${project}`);
            console.log(`Found ${Object.keys(projects).length} total projects in JSON and ${parsedProjects.length} are internal`);
            // Consolidate projects
            await cleanUpDuplicateProjects(qx, parsedProjects, parameters.dryRun || false);
            console.log('Project cleanup completed successfully');
            process.exit(0);
        }
        catch (error) {
            console.error('Error during project cleanup:', error);
            process.exit(1);
        }
    });
}
//# sourceMappingURL=cleanup-duplicate-insights-projects.js.map