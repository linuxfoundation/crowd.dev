"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.memberCustomViews = void 0;
const types_1 = require("@crowd/types");
const newAndActive = {
    name: 'New and active',
    config: {
        search: '',
        relation: 'and',
        order: {
            prop: 'lastActive',
            order: 'descending',
        },
        settings: {
            bot: 'exclude',
            teamMember: 'exclude',
            organization: 'exclude',
        },
        joinedDate: {
            operator: 'gt',
            value: 'lastMonth',
        },
        lastActivityDate: {
            operator: 'gt',
            value: 'lastMonth',
        },
    },
    visibility: types_1.CustomViewVisibility.TENANT,
    placement: types_1.CustomViewPlacement.MEMBER,
};
const slippingAway = {
    name: 'Slipping away',
    config: {
        search: '',
        relation: 'and',
        order: {
            prop: 'lastActive',
            order: 'descending',
        },
        settings: {
            bot: 'exclude',
            teamMember: 'exclude',
            organization: 'exclude',
        },
        engagementLevel: {
            value: ['fan', 'ultra'],
            include: true,
        },
        lastActivityDate: {
            operator: 'lt',
            value: 'lastMonth',
        },
    },
    visibility: types_1.CustomViewVisibility.TENANT,
    placement: types_1.CustomViewPlacement.MEMBER,
};
const mostEngaged = {
    name: 'Most engaged',
    config: {
        search: '',
        relation: 'and',
        order: {
            prop: 'lastActive',
            order: 'descending',
        },
        settings: {
            bot: 'exclude',
            teamMember: 'exclude',
            organization: 'exclude',
        },
        engagementLevel: {
            value: ['fan', 'ultra'],
            include: true,
        },
    },
    visibility: types_1.CustomViewVisibility.TENANT,
    placement: types_1.CustomViewPlacement.MEMBER,
};
const influential = {
    name: 'Influential',
    config: {
        search: '',
        relation: 'and',
        order: {
            prop: 'lastActive',
            order: 'descending',
        },
        settings: {
            bot: 'exclude',
            teamMember: 'exclude',
            organization: 'exclude',
        },
        reach: {
            operator: 'gte',
            value: 500,
        },
    },
    visibility: types_1.CustomViewVisibility.TENANT,
    placement: types_1.CustomViewPlacement.MEMBER,
};
const teamMembers = {
    name: 'Team contacts',
    config: {
        search: '',
        relation: 'and',
        order: {
            prop: 'lastActive',
            order: 'descending',
        },
        settings: {
            bot: 'exclude',
            teamMember: 'filter',
            organization: 'exclude',
        },
    },
    visibility: types_1.CustomViewVisibility.TENANT,
    placement: types_1.CustomViewPlacement.MEMBER,
};
exports.memberCustomViews = [
    newAndActive,
    slippingAway,
    mostEngaged,
    influential,
    teamMembers,
];
//# sourceMappingURL=member.js.map