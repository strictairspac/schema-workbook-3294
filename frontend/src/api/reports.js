import {api} from './client.js';
export const reportsApi={
  list:()=>api('/reports'),
  create:data=>api('/reports','POST',data),
  overview:()=>api('/reports/storage/overview'),
  retry:id=>api('/reports/'+id+'/retry','POST'),
};
