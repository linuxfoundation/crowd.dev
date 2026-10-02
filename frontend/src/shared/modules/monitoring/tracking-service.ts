import authAxios from '@/shared/axios/auth-axios';
import type { Session } from './types/session';
import type { Event } from './types/event';

export const createSession: (session: Session) => Promise<Session> = async (session: Session) => {
  const response = await authAxios.post(
    '/product/session',
    {
      ...session,
      excludeSegments: true,
    },
  );

  return response.data;
};

export const updateSession = async (id: string, endTime: string) => {
  const response = await authAxios.put(
    `/product/session/${id}`,
    {
      endTime,
      excludeSegments: true,
    },
  );

  return response.data;
};

export const createEvent = async (event: Event) => {
  const response = await authAxios.post(
    '/product/event',
    {
      ...event,
      excludeSegments: true,
    },
  );

  return response.data;
};
