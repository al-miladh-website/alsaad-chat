import express from 'express';
import http from 'http';
import {Server} from 'socket.io';
import crypto from 'crypto';
import 'dotenv/config';
const app=express(), server=http.createServer(app), io=new Server(server);
app.use(express.static('public'));
const rooms=new Map();
const limit=500;
async function translate(text,from,to){
  if(!process.env.TRANSLATE_URL) return {text,translated:false};
  const controller=new AbortController(); const timeout=setTimeout(()=>controller.abort(),12000);
  try{
    const res=await fetch(process.env.TRANSLATE_URL,{method:'POST',headers:{'Content-Type':'application/json',...(process.env.TRANSLATE_API_KEY?{'Authorization':`Bearer ${process.env.TRANSLATE_API_KEY}`}:{})},body:JSON.stringify({q:text,source:from,target:to,format:'text'}),signal:controller.signal});
    if(!res.ok) throw Error('translation unavailable');
    const data=await res.json();
    return {text:data.translatedText||text,translated:!!data.translatedText};
  }catch{return {text,translated:false};}finally{clearTimeout(timeout);}
}
io.on('connection',socket=>{
 socket.on('join',({room,secret,role},ack)=>{
  if(typeof room!=='string'||!/^\d{6}$/.test(room)||typeof secret!=='string'||secret.length<8||secret.length>100||!['ar','ckb'].includes(role))return ack?.({ok:false,error:'بيانات الغرفة غير صحيحة'});
  const digest=crypto.createHash('sha256').update(secret).digest('hex');
  let state=rooms.get(room);
  if(state&&state.digest!==digest)return ack?.({ok:false,error:'رمز الغرفة غير صحيح'});
  if(!state){state={digest,members:new Set()};rooms.set(room,state)}
  if(state.members.size>=2&&!state.members.has(socket.id))return ack?.({ok:false,error:'الغرفة ممتلئة'});
  if(socket.data.room)socket.leave(socket.data.room);
  socket.data.room=room;socket.data.role=role;state.members.add(socket.id);socket.join(room);
  ack?.({ok:true});io.to(room).emit('presence',{count:state.members.size});
 });
 socket.on('message',async(text,ack)=>{
  const room=socket.data.room, role=socket.data.role;
  if(!room||typeof text!=='string'||!text.trim()||text.length>limit)return ack?.({ok:false});
  const original=text.trim(), id=crypto.randomUUID(), target=role==='ar'?'ckb':'ar';
  const result=await translate(original,role,target);
  io.to(room).emit('message',{id,from:role,original,translated:result.text,translatedOk:result.translated,time:Date.now()});ack?.({ok:true});
 });
 socket.on('disconnect',()=>{const room=socket.data.room;if(!room)return;const state=rooms.get(room);if(!state)return;state.members.delete(socket.id);io.to(room).emit('presence',{count:state.members.size});if(!state.members.size)rooms.delete(room)});
});
server.listen(process.env.PORT||3000,()=>console.log('DIL Chat running'));
