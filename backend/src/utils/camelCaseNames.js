"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.default = (name) => {
    // remove all characters other than [space, a-z, 0-9]
    const split = name
        .toLowerCase()
        .replace(/[^a-z0-9 ]/gi, '')
        .split(' ');
    let camelCaseName = '';
    for (const word of split) {
        camelCaseName += word.charAt(0).toUpperCase() + word.slice(1);
    }
    return camelCaseName.charAt(0).toLowerCase() + camelCaseName.slice(1);
};
//# sourceMappingURL=camelCaseNames.js.map