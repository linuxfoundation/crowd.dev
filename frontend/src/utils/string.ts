export const withHttp = (url: string): string => (!/^https?:\/\//i.test(url) ? `https://${url}` : url);

export const toSentenceCase = (string: string): string => string.charAt(0).toUpperCase() + string.slice(1);

export const extractRepoNameFromUrl = (url: string): string => {
  const regex = /^https?:\/\/github\.com\/(.+?)\/(.+?)(?:\.git)?$/;
  const match = url.match(regex);

  if (match) {
    return match[2];
  }
  return url;
};

export const snakeToSentenceCase = (string: string): string => string.split('_')
  .map((word: string): string => word.charAt(0).toUpperCase() + word.slice(1)).join(' ');

export const truncateText = (text: string, characters: number = 200, suffix: string = ''): string => {
  if (text.length > characters) {
    return `${text.substring(0, characters)}${suffix}`;
  }
  return text;
};

/** trim and replace multiple/double spaces with a single space
 * '   Emma  Ray   ' ==> 'Emma Ray
 * 'Emma  Ray' ==> 'Emman Ray
*/
export const trimAndReduceSpaces = (str: string): string => str.trim().replace(/\s+/g, ' ');
