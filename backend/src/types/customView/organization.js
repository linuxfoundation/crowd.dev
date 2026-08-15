"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.organizationCustomViews = void 0;
const types_1 = require("@crowd/types");
const newAndActiveOrgs = {
    name: 'New and active',
    config: {
        search: '',
        relation: 'and',
        order: {
            prop: 'joinedAt',
            order: 'descending',
        },
        settings: {
            teamOrganization: 'exclude',
        },
        joinedDate: {
            operator: 'gt',
            value: 'lastMonth',
        },
    },
    visibility: types_1.CustomViewVisibility.TENANT,
    placement: types_1.CustomViewPlacement.ORGANIZATION,
};
const mostMembers = {
    name: 'Most contacts',
    config: {
        search: '',
        relation: 'and',
        order: {
            prop: 'memberCount',
            order: 'descending',
        },
        settings: {
            teamOrganization: 'exclude',
        },
    },
    visibility: types_1.CustomViewVisibility.TENANT,
    placement: types_1.CustomViewPlacement.ORGANIZATION,
};
const teamOrganizations = {
    name: 'Team organizations',
    config: {
        search: '',
        relation: 'and',
        order: {
            prop: 'lastActive',
            order: 'descending',
        },
        settings: {
            teamOrganization: 'filter',
        },
    },
    visibility: types_1.CustomViewVisibility.TENANT,
    placement: types_1.CustomViewPlacement.ORGANIZATION,
};
exports.organizationCustomViews = [
    newAndActiveOrgs,
    mostMembers,
    teamOrganizations,
];
//# sourceMappingURL=organization.js.map