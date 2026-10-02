import type { CollectionModel } from '../models/collection.model';

export interface CollectionsState {
  collections: CollectionModel[];
}

const state: CollectionsState = {
  collections: [],
};

export default () => state;
