function isUrl(url: string): boolean {
  const urlPattern = new RegExp(
    '^(https?:\\/\\/)?' // validate protocol
      + '((([a-z\\d]([a-z\\d-]*[a-z\\d])*)\\.)+[a-z]{2,}|' // validate domain name
      + '((\\d{1,3}\\.){3}\\d{1,3}))' // validate OR ip (v4) address
      + '(\\:\\d+)?(\\/[-a-z\\d%_.~+]*)*' // validate port and path
      + '(\\?[;&a-z\\d%_.~+=-]*)?' // validate query string
      + '(\\#[-a-z\\d_]*)?$',
    'i',
  ); // validate fragment locator

  // Check the URL is a valid URL
  return !!urlPattern.test(url);
}

export default isUrl;
