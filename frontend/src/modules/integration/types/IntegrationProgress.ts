export interface IntegrationProgress {
  data: Record<string, IntegrationProgressPart>;
  percentage?: number;
  platform: string;
  reportStatus: 'ok' | 'in-progress' | 'calculating';
  segmentId: string;
  segmentName: string;
  type: string;
}

export interface IntegrationProgressPart {
  message: string;
  percentage: number;
  status: 'ok' | 'in-progress';
}
