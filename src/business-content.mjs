// Business cards are display content, never executable actions or payment flows.
// Native-flow JSON, buttons, URLs and thumbnails are deliberately not projected.
export function businessContent(body) {
  const template=body?.templateMessage;
  const hydrated=template?.hydratedTemplate||template?.hydratedFourRowTemplate;
  const interactive=body?.interactiveMessage||template?.interactiveMessageTemplate;
  const buttons=body?.buttonsMessage;
  if(!template&&!interactive&&!buttons)return undefined;
  if(hydrated?.maskLinkedDevices)return null;
  const header=interactive?.header||hydrated||buttons||template?.fourRowTemplate||{};
  if(header.imageMessage?.viewOnce||header.videoMessage?.viewOnce)return null;
  const texts=interactive?[header.title,header.subtitle,interactive.body?.text,interactive.footer?.text]:hydrated?[hydrated.hydratedTitleText,hydrated.hydratedContentText,hydrated.hydratedFooterText]:buttons?[buttons.text,buttons.contentText,buttons.footerText]:[];
  const strings=texts.filter(value=>typeof value==='string'&&value.length);
  const text=strings.length?strings.join('\n\n'):'Business message · open in WhatsApp';
  const contexts=[template?.contextInfo,interactive?.contextInfo,buttons?.contextInfo,header.imageMessage?.contextInfo,header.videoMessage?.contextInfo,header.documentMessage?.contextInfo];
  let expiration=0;
  for(const context of contexts){const seconds=Number(context?.expiration||0);if(!Number.isSafeInteger(seconds)||seconds<0)return null;if(seconds)expiration=expiration?Math.min(expiration,seconds):seconds;}
  const contextInfo={expiration};
  if(header.imageMessage)return {imageMessage:{...header.imageMessage,caption:text,contextInfo}};
  if(header.videoMessage)return {videoMessage:{...header.videoMessage,caption:text,contextInfo}};
  if(header.documentMessage)return {documentMessage:{...header.documentMessage,caption:text,contextInfo}};
  return {extendedTextMessage:{text,contextInfo}};
}
