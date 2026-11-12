import type { ConfigType } from 'dayjs';
import config from '@/config';
import type { Tenant } from '@/modules/auth/types/Tenant.type';
import { dateHelper } from '@/shared/date-helper/date-helper';

/**
 * Time ago utility
 *
 * This is a small wrapper of the dateHelper(date).fromNow() method, to handle our data-related exception regarding
 * the year 1970, replacing the default label ("52 years ago") to "some time ago".
 */
export const formatDateToTimeAgo = (timestamp: ConfigType): string => (dateHelper.utc(timestamp).year() < 2000
  ? 'some time ago'
  : dateHelper.utc(timestamp).fromNow());

export const formatDateToTimeAgoForIntegrations = (timestamp: ConfigType): string => dateHelper().to(dateHelper(timestamp));

export const formatDate = ({
  timestamp = null,
  subtractDays,
  subtractMonths,
  subtractYears,
  format = 'YYYY-MM-DD',
}: {
  timestamp?: ConfigType;
  subtractDays?: number | null;
  subtractMonths?: number | null;
  subtractYears?: number | null;
  format?: string;
}): string => {
  const date = timestamp ? dateHelper(timestamp) : dateHelper();

  if (subtractDays) {
    date.subtract(subtractDays, 'days');
  }

  if (subtractMonths) {
    date.subtract(subtractMonths, 'months');
  }

  if (subtractYears) {
    date.subtract(subtractYears, 'years');
  }

  return date.format(format);
};

export const getTrialDate = (tenant: Pick<Tenant, 'isTrialPlan' | 'trialEndsAt'>): string | null => {
  if ((config as typeof config & { isCommunityVersion: boolean }).isCommunityVersion || !tenant.isTrialPlan) {
    return null;
  }

  const daysLeft = dateHelper(tenant.trialEndsAt).diff(
    dateHelper(),
    'days',
  );

  return `Trial (${daysLeft < 0 ? 0 : daysLeft} days left)`;
};

export const isCurrentDateAfterGivenWorkingDays = (date: string | number | Date, workingDays: number = 3): boolean => {
  const givenDate = new Date(date);
  let workingDaysAdded = 0;

  while (workingDaysAdded < workingDays) {
    givenDate.setDate(givenDate.getDate() + 1);

    // Check if the current day is a weekend day (Saturday or Sunday)
    const isWeekendDay = givenDate.getDay() === 0 || givenDate.getDay() === 6;

    if (!isWeekendDay) {
      workingDaysAdded += 1;
    }
  }

  return new Date() > givenDate;
};
