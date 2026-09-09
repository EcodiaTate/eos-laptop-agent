import puppeteer from 'puppeteer-core';
try{
const b = await puppeteer.connect({browserURL:'http://127.0.0.1:9222', defaultViewport:null});
const p = await b.newPage();
const errs=[]; p.on('console',m=>{if(m.type()==='error')errs.push(m.text().slice(0,180));});
p.on('pageerror',e=>errs.push('PAGEERR '+e.message.slice(0,180)));
await p.goto('http://localhost:3000/next-world-grant',{waitUntil:'domcontentloaded',timeout:60000});
await new Promise(r=>setTimeout(r,3000));
console.log('CONSOLE_ERRORS '+errs.length);
errs.slice(0,6).forEach(e=>console.log(' - '+e));
b.disconnect();
}catch(e){console.log('ERR '+e.message);}
