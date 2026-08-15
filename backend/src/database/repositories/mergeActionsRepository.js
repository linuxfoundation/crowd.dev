"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MergeActionsRepository = void 0;
const sequelize_1 = require("sequelize");
const types_1 = require("@crowd/types");
const sequelizeRepository_1 = __importDefault(require("./sequelizeRepository"));
class MergeActionsRepository {
    static async findById(id, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const record = await options.database.sequelize.query(`
      SELECT *
      FROM "mergeActions"
      WHERE id = :id;
      `, {
            replacements: { id },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        if (record.length === 1) {
            const data = record[0];
            // fix old identities to use the new format
            if (data.type === types_1.MergeActionType.MEMBER && data.unmergeBackup) {
                const backup = data.unmergeBackup;
                if (backup.primary) {
                    for (const identity of backup.primary.identities) {
                        if ('username' in identity) {
                            identity.value = identity.username;
                            identity.type = types_1.MemberIdentityType.USERNAME;
                            delete identity.username;
                        }
                    }
                }
                if (backup.secondary) {
                    for (const identity of backup.secondary.identities) {
                        if ('username' in identity) {
                            identity.value = identity.username;
                            identity.type = types_1.MemberIdentityType.USERNAME;
                            delete identity.username;
                        }
                    }
                }
            }
            return data;
        }
        return null;
    }
    static async findMergeBackup(primaryMemberId, type, identity, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        let records;
        if (type === types_1.MergeActionType.MEMBER) {
            const memberIdentity = identity;
            records = await options.database.sequelize.query(`
        select *
        from "mergeActions" ma
        where ma."primaryId" = :primaryMemberId
          and exists(
                select 1
                from jsonb_array_elements(ma."unmergeBackup" -> 'secondary' -> 'identities') as identities
                where (identities ->> 'username' = :secondaryMemberIdentityValue or (identities ->> 'type' = :secondaryMemberIdentityType and identities ->> 'value' = :secondaryMemberIdentityValue))
                  and identities ->> 'platform' = :secondaryMemberIdentityPlatform
            );
        `, {
                replacements: {
                    primaryMemberId,
                    secondaryMemberIdentityValue: memberIdentity.value,
                    secondaryMemberIdentityType: memberIdentity.type,
                    secondaryMemberIdentityPlatform: memberIdentity.platform,
                },
                type: sequelize_1.QueryTypes.SELECT,
                transaction,
            });
            // fix old identities to use the new format
            for (const record of records) {
                const data = record;
                // fix old identities to use the new format
                if (data.type === types_1.MergeActionType.MEMBER && data.unmergeBackup) {
                    const backup = data.unmergeBackup;
                    if (backup.primary) {
                        for (const identity of backup.primary.identities) {
                            if ('username' in identity) {
                                identity.value = identity.username;
                                identity.type = types_1.MemberIdentityType.USERNAME;
                                delete identity.username;
                            }
                        }
                    }
                    if (backup.secondary) {
                        for (const identity of backup.secondary.identities) {
                            if ('username' in identity) {
                                identity.value = identity.username;
                                identity.type = types_1.MemberIdentityType.USERNAME;
                                delete identity.username;
                            }
                        }
                    }
                }
            }
        }
        else if (type === types_1.MergeActionType.ORG) {
            const organizationIdentity = identity;
            records = await options.database.sequelize.query(`
        select *
        from "mergeActions" ma
        where ma."primaryId" = :primaryMemberId
          and exists(
                select 1
                from jsonb_array_elements(ma."unmergeBackup" -> 'secondary' -> 'identities') as identities
                where (identities ->> 'name' = :secondaryOrgIdentityValue or (identities ->> 'type' = :secondaryOrgIdentityType and identities ->> 'value' = :secondaryOrgIdentityValue))
                  and identities ->> 'platform' = :secondaryOrgIdentityPlatform
            );
        `, {
                replacements: {
                    primaryMemberId,
                    secondaryOrgIdentityType: organizationIdentity.type,
                    secondaryOrgIdentityValue: organizationIdentity.value,
                    secondaryOrgIdentityPlatform: organizationIdentity.platform,
                },
                type: sequelize_1.QueryTypes.SELECT,
                transaction,
            });
            // fix old identities to use the new format
            for (const record of records) {
                const data = record;
                // fix old identities to use the new format
                if (data.type === types_1.MergeActionType.ORG && data.unmergeBackup) {
                    const backup = data.unmergeBackup;
                    if (backup.primary) {
                        for (const identity of backup.primary.identities) {
                            if ('name' in identity) {
                                identity.value = identity.name;
                                identity.type = types_1.OrganizationIdentityType.USERNAME;
                                delete identity.name;
                            }
                        }
                        if (backup.primary.website) {
                            backup.primary.identities.push({
                                type: types_1.OrganizationIdentityType.PRIMARY_DOMAIN,
                                value: backup.primary.website,
                                platform: 'custom',
                                verified: true,
                                source: null,
                                sourceId: null,
                                integrationId: null,
                            });
                        }
                        if (backup.primary.alternativeDomains) {
                            for (const domain of backup.primary.alternativeDomains) {
                                backup.primary.identities.push({
                                    type: types_1.OrganizationIdentityType.ALTERNATIVE_DOMAIN,
                                    value: domain,
                                    platform: 'enrichment',
                                    verified: false,
                                    source: null,
                                    sourceId: null,
                                    integrationId: null,
                                });
                            }
                        }
                        if (backup.primary.affiliatedProfiles) {
                            for (const profile of backup.primary.affiliatedProfiles) {
                                backup.primary.identities.push({
                                    type: types_1.OrganizationIdentityType.AFFILIATED_PROFILE,
                                    value: profile,
                                    platform: 'enrichment',
                                    verified: false,
                                    source: null,
                                    sourceId: null,
                                    integrationId: null,
                                });
                            }
                        }
                    }
                    if (backup.secondary) {
                        for (const identity of backup.secondary.identities) {
                            if ('name' in identity) {
                                identity.value = identity.name;
                                identity.type = types_1.OrganizationIdentityType.USERNAME;
                                delete identity.name;
                            }
                        }
                        if (backup.secondary.website) {
                            backup.secondary.identities.push({
                                type: types_1.OrganizationIdentityType.PRIMARY_DOMAIN,
                                value: backup.secondary.website,
                                platform: 'custom',
                                verified: true,
                                source: null,
                                sourceId: null,
                                integrationId: null,
                            });
                        }
                        if (backup.secondary.alternativeDomains) {
                            for (const domain of backup.secondary.alternativeDomains) {
                                backup.secondary.identities.push({
                                    type: types_1.OrganizationIdentityType.ALTERNATIVE_DOMAIN,
                                    value: domain,
                                    platform: 'enrichment',
                                    verified: false,
                                    source: null,
                                    sourceId: null,
                                    integrationId: null,
                                });
                            }
                        }
                        if (backup.secondary.affiliatedProfiles) {
                            for (const profile of backup.secondary.affiliatedProfiles) {
                                backup.secondary.identities.push({
                                    type: types_1.OrganizationIdentityType.AFFILIATED_PROFILE,
                                    value: profile,
                                    platform: 'enrichment',
                                    verified: false,
                                    source: null,
                                    sourceId: null,
                                    integrationId: null,
                                });
                            }
                        }
                    }
                }
            }
        }
        if (records.length === 0) {
            return null;
        }
        return records[0];
    }
}
exports.MergeActionsRepository = MergeActionsRepository;
//# sourceMappingURL=mergeActionsRepository.js.map