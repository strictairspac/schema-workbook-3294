import {reportsApi} from '../api/reports.js';
export const reportsStore={
  items:[],
  overview:null,
  async refresh(){
    const [items,overview]=await Promise.all([reportsApi.list(),reportsApi.overview()]);
    this.items=items;
    this.overview=overview;
    return this.items;
  },
};
