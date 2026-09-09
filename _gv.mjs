import puppeteer from 'puppeteer-core';
const OUT='/private/tmp/claude-501/-Users-ecodia--code-ecodiaos-backend/4244eaeb-2903-45a2-98b6-72da9ae15862/scratchpad/';
try{
const b = await puppeteer.connect({browserURL:'http://127.0.0.1:9222', defaultViewport:null});
const p = await b.newPage();
// DESKTOP
await p.setViewport({width:1440,height:1000});
await p.goto('http://localhost:3000/next-world-grant',{waitUntil:'domcontentloaded',timeout:45000});
await new Promise(r=>setTimeout(r,2500));
const err = await p.evaluate(()=>{const n=document.querySelector('nextjs-portal,[data-nextjs-dialog]');return n?n.textContent.slice(0,160):'';});
console.log('BUILDERR:'+(err||'none'));
await p.screenshot({path:`${OUT}gv-desktop-full.png`, fullPage:true});
// functional: does the share card render to a valid PNG?
const png = await p.evaluate(()=>{
  const c=document.createElement('canvas'); // ensure fn exists via clicking overlay instead
  return null;
});
// open the share overlay: find + click the "Save a share card" button
const clicked = await p.evaluate(()=>{
  const b=[...document.querySelectorAll('button')].find(x=>/save a share card/i.test(x.textContent||''));
  if(b){b.click();return true;} return false;
});
console.log('overlay-btn-clicked:'+clicked);
await new Promise(r=>setTimeout(r,900));
// check canvas produced a non-trivial PNG
const cardInfo = await p.evaluate(()=>{
  const c=document.querySelector('.nwg-card-canvas'); if(!c) return {found:false};
  const d=c.toDataURL('image/png'); return {found:true, w:c.width, h:c.height, bytes:d.length};
});
console.log('CARD:'+JSON.stringify(cardInfo));
await p.screenshot({path:`${OUT}gv-overlay.png`});
// close overlay
await p.evaluate(()=>{const b=[...document.querySelectorAll('.nwg-overlay button')].find(x=>/close/i.test(x.textContent||''));if(b)b.click();});
// MOBILE
await p.setViewport({width:390,height:844,isMobile:true,deviceScaleFactor:2});
await p.goto('http://localhost:3000/next-world-grant',{waitUntil:'domcontentloaded',timeout:45000});
await new Promise(r=>setTimeout(r,2200));
await p.screenshot({path:`${OUT}gv-mobile-full.png`, fullPage:true});
console.log('OK');
await p.close(); b.disconnect();
}catch(e){console.log('ERR '+e.message);}
