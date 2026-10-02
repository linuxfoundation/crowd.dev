# 📚 UI-Kit Structure Documentation

This document describes the current folder structure of the **UI-Kit** located in `src/ui-kit/`. This helps developers understand how shared UI components are organized.

---

## 🗂️ Overview

The `ui-kit` folder in our project (`src/ui-kit/`) acts as our **shared UI component library**. It contains reusable Vue components, styles, types, and Storybook stories to build consistent and maintainable UIs across the app.

---

## 📁 Folder Structure
```text
src/
└── ui-kit/
    ├── <component-name>/
    │   ├── <ComponentName>.vue
    │   ├── <component-name>.scss
    │   ├── <ComponentName>.stories.ts
    │   ├── types/ (optional)
    │   │   └── <ComponentName>Type.ts
    └── ...

---
```

## 🔎 Component Folder Structure

Each component folder typically contains:

- **ComponentName.vue** — The main Vue component file.
- **component-name.scss** — SCSS file containing styles for the component.
- **ComponentName.stories.ts** — Storybook stories for interactive documentation and development.
- **types/** (optional) — TypeScript type definitions specific to this component.


---

## 📦 Examples of Components

Here are some examples of components currently in the `ui-kit`:

- **avatar/**
  - `Avatar.vue`
  - `avatar.scss`
  - `Avatar.stories.ts`
- **button/**
  - `Button.vue`
  - `button.scss`
  - `Button.stories.ts`
  - `ButtonGroup.vue`
- **card/**
  - `Card.vue`
  - `card.scss`
  - `Card.stories.ts`
- **dropdown/**
  - `Dropdown.vue`
  - `dropdown.scss`
  - `Dropdown.stories.ts`
  - `DropdownItem.vue`
  - `DropdownSeparator.vue`
- **field-message/**
  - `FieldMessage.vue`
  - `field-message.scss`
  - `FieldMessage.stories.ts`
  - `types/FieldMessageType.ts`
  - `constants/fieldMessageTypeData.ts`
- **table/**
  - `Table.vue`
  - `table.scss`
  - `Table.stories.ts`
  - `TableCell.vue`
  - `TableHead.vue`

(...and many more.)

---

## 📝 Notes

**Storybook Integration:**  
Each component generally includes a `.stories.ts` file to support Storybook documentation.

**Every component ships a story:**  
A `.stories.ts` file is required for every new ui-kit component, not optional — see the [Storybook Guide](./Storybook-Guide.md) for how to write one.

**Styles:**  
Component-specific SCSS is stored in files like `component-name.scss` for modular styling.

**Types:**  
Some components use a `types/` folder to define TypeScript types, e.g. `FieldMessageType.ts`.

---

## 🛠️ Adding New Components

If you’re adding a new component:
1. Create a new folder in `src/ui-kit/`.
2. Add:
   - `<ComponentName>.vue`
   - `<component-name>.scss`
   - `<ComponentName>.stories.ts`
   - `types/<ComponentName>Type.ts` (if needed)
3. Import the component *.scss file in `src/ui-kit/index.scss` file
4. Import the component in Storybook and other relevant parts of the app.

---

## 📌 Conclusion

Following this structure ensures consistency, maintainability, and a scalable design system for our project. For any questions, please reach out to the frontend team.

