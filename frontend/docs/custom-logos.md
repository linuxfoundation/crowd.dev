# Step-by-step guide to add logos to custom platforms

This is a step-by-step guide to add logo images to custom platforms. Currently, we only render logo images for default integrations/platforms. For custom platforms we render fallback icons.

Platform logos are rendered in two formats, png/jpeg and svgs. Each format is used in different places of the UI, so there are two main components responsible to handle their display, `platform.vue` and `svg.vue`. For each new custom platform logo, the logo should be added in both formats.

The following steps provide a possible approach to add custom logos to custom platforms:

1. Inside `frontend/src/integrations` folder:

   1. Create a `/custom` folder to hold all custom platforms _if it's the first time adding a custom platform_.
   2. Each new custom platform folder should mirror the structure of the existing platforms. It should contain a `config.js` and a `index.js` file.
      1. `config.js` file should only contain 3 properties, `image`, `name`, and `url`.
      2. E.g. If we want to add a custom logo for `Luma` platform it would require:
         1. Folder structure: `src/integrations/custom/luma`
         2. File structure:
            1. `frontend/src/integrations/custom/luma/config.js`
            2. `frontend/src/integrations/custom/luma/index.js`
         3. Content of `config.js`:
            1. `image` can be a directory or a link.
            2. `url` is necessary as it will prevent some components to break
         ```
            export default {
                name: 'Luma',
                image: '/src/assets/images/integrations/custom/luma.png',
                url: () => null,
            };
         ```

2. Add the new images to the project:
   1. If you choose to add an image via a directory, add it to the `frontend/public/src/assets/images/integrations/custom` folder. Create a`/custom` folder _if it's the first time adding a custom logo_.
   2. Support the svg logo by adding it to the `frontend/public/icons/crowd-icons.svg` file. E.g.:
      ```
      <symbol id="luma">
      	<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg">
            ...
        </svg>
      </symbol>
      ```
      1. Add the `symbol` on the same level as the existing ones. Name the svg `symbol` with the key name of your platform. E.g. `id="luma"`
3. In `frontend/src/integrations/integrations-config.js` file you will need the following:

   1. Import the custom platform you just added:

   ```
   import luma from './custom/luma';
   ```

   2. Add `customIntegrations` method to the file. If it is already created, add the newly created platform to the object:

   ```
   get customIntegrations() {
       return {
           luma,
       };
   }
   ```

   3. Update `getConfig` method _if it's the first time adding custom platforms_:

   ```
   getConfig(platform) {
       if (this.integrations[platform]) {
           return this.integrations[platform];
       }

       return this.customIntegrations[platform];
   }
   ```

## Contributing

- [Guidelines](https://github.com/linuxfoundation/crowd.dev/blob/main/CONTRIBUTING.md)
- Open a Pull Request to `crowd-linux` branch. Requires at least 1 approval before merging.

## Testing

To test if the new custom logo is properly set, add a custom activity with a custom platform. The following steps will guide you through the testing phase:

1. Run the application locally and all the required services with `./cli clean-start-dev` from the `/scripts` folder
   1. For the frontend it is advisable to run outside docker since it will compile faster. You can run frontend from the `/frontend` folder with `npm run start:dev:local`. Don't forget to stop the frontend container before this.
2. Create a custom activity using the following endpoint and a similar payload:
   1. `tenantId` can be found in the settings page
   2. `token` can be obtained through the `sign-in` endpoint
   3. For the `body` payload:
      1. `member` can be existent or new
      2. `type` represents the activity type, and `platform` needs to match the platform key configured for the custom logo

```
URL: http://localhost:8081/api/activity/with-member
METHOD: POST
HEADERS: {
    "Authorization": "{token}"",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

BODY:
{
    "member": {
        "username": "john doe",
        "displayName": "Jonh Doe",
        "email": "johndoe@lfx.dev",
        "joinedAt": "2023-05-26"
    },
    "sourceId": "123",
    "type": "registered-to-an-event",
    "platform": "luma",
    "timestamp": "2023-05-26T11:52:55.274Z",
    "title": "Dummy title",
    "body": "Dummy body",
    "channel": "Channel",
    "sourceParentId": null
}
```

3. After creating the activity you will need to check if the custom logo is properly configured in 2 different places:
   1. In "Activities" page search for the newly created activity. The logo should be rendered before the activity type.
   2. In "Members" page and Member's profile page, look for the member who has the associated custom activity. The custom platform should now be added as an identity with the custom logo.
