/**
 * Bug from element-plus on select components
 * Issue: When mouse leaves select popper,
 * hover class remains in select options
 * This method should be added to @mouseleave in
 * el-option components
 */
export const onSelectMouseLeave = (el: MouseEvent): void => {
  (el.target as HTMLElement).classList.remove('hover');
};
