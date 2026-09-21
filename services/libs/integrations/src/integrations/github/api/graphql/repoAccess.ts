import { Repo } from '../../types'

import BaseQuery from './baseQuery'

/* eslint class-methods-use-this: 0 */
class RepoAccessQuery extends BaseQuery {
  repo: Repo

  constructor(repo: Repo, githubToken: string) {
    const repoAccessQuery = `{
            repository(owner: "${repo.owner}", name: "${repo.name}") {
              id
            }
          }`

    super(githubToken, repoAccessQuery, 'repository', 1)

    this.repo = repo
  }

  getEventData() {
    return { hasPreviousPage: false, startCursor: null, data: [{}] }
  }
}

export default RepoAccessQuery
