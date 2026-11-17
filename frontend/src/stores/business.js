import {loadAll} from '../api/business.js';
export const store={session:JSON.parse(localStorage.getItem('session')||'null'),data:{sources:[],periods:[],policies:[],runs:[],schedules:[],deliveries:[],events:[],faults:{points:[],rules:[]},audits:[]},selected:null};
export async function reload(){
 const all=await loadAll();
 store.data={...store.data,...all,faults:all.faults??{points:[],rules:[]}};
}
export function setSession(value){store.session=value;if(value)localStorage.setItem('session',JSON.stringify(value));else localStorage.removeItem('session')}
