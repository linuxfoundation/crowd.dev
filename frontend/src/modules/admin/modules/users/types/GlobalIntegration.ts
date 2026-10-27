export interface GlobalIntegrationsQuery {
  limit: number;
  offset: number;
  query: string;
  platform?: string;
  status: string[];
  segment?: string;
  isLF?: boolean;
}

export interface GlobalIntegrationStatusCountQuery {
  platform?: string;
  segment?: string;
  isLF?: boolean;
}

export interface GlobalIntegrationStatusCount {
  status: string;
  count: string | number;
}
