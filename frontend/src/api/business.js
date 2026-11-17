import {request} from './client.js';
export const loadAll=async()=>Object.fromEntries(await Promise.all(['sources','periods','policies','runs','schedules','deliveries','delivery-events','faults','audits'].map(async k=>[k==='delivery-events'?'events':k,await request('/'+k)])));
export const saveSource=(data,id)=>request('/sources'+(id?'/'+id:''),id?'PUT':'POST',{data});
export const savePolicy=data=>request('/policies','POST',{data});
export const submitRun=data=>request('/runs','POST',data);
export const approveRun=(id,note)=>request('/runs/'+id+'/approve','POST',{note});
export const setFault=(point,enabled,note='')=>request('/faults','POST',{point,enabled,note});
export const resetFaults=()=>request('/faults/reset','POST',{});
