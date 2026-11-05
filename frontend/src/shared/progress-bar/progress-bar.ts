import NProgress from 'nprogress';
import 'nprogress/nprogress.css';

NProgress.configure({ showSpinner: false });

export function start(): void {
  NProgress.start();
}

export function done(): void {
  NProgress.done();
}
