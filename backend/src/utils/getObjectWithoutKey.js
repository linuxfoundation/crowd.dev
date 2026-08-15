"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * Utility functon that removes given key(s) from an object.
 * Returns a new object with given key(string) or keys(string[]) removed.
 * @param object object to be modified
 * @param key key to be removed / or keys string arrays to be removed
 * @returns the new object without given keys
 *
 */
exports.default = (object, key) => {
    let objectWithoutKeys;
    if (typeof key === 'string') {
        const { [key]: _, ...otherKeys } = object;
        objectWithoutKeys = otherKeys;
    }
    else if (Array.isArray(key)) {
        objectWithoutKeys = key.reduce((acc, i) => {
            const { [i]: _, ...otherKeys } = acc;
            acc = otherKeys;
            return acc;
        }, object);
    }
    return objectWithoutKeys;
};
//# sourceMappingURL=getObjectWithoutKey.js.map