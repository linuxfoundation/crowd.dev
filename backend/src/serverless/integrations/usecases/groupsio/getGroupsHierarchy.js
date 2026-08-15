"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getGroupsHierarchy = void 0;
const getGroupsHierarchy = (groups) => {
    const hierarchy = {};
    groups.forEach((group) => {
        const [mainGroupSlug, subGroupSlug] = group.slug.split('+');
        if (!hierarchy[mainGroupSlug]) {
            hierarchy[mainGroupSlug] = {
                mainGroup: null,
                subGroups: [],
            };
        }
        if (subGroupSlug) {
            hierarchy[mainGroupSlug].subGroups.push(group);
        }
        else {
            hierarchy[mainGroupSlug].mainGroup = group;
        }
    });
    return hierarchy;
};
exports.getGroupsHierarchy = getGroupsHierarchy;
//# sourceMappingURL=getGroupsHierarchy.js.map