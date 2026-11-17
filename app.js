var Q0=Object.defineProperty,X0=Object.defineProperties;var J0=Object.getOwnPropertyDescriptors;var oh=Object.getOwnPropertySymbols;var eC=Object.prototype.hasOwnProperty,tC=Object.prototype.propertyIsEnumerable;var ih=(e,n,t)=>n in e?Q0(e,n,{enumerable:!0,configurable:!0,writable:!0,value:t}):e[n]=t,v=(e,n)=>{for(var t in n||={})eC.call(n,t)&&ih(e,t,n[t]);if(oh)for(var t of oh(n))tC.call(n,t)&&ih(e,t,n[t]);return e},T=(e,n)=>X0(e,J0(n));var wi=[null],as=[!1],wr=1,nC=null,Be=Symbol("SIGNAL");function k(e){let n=wi[0];return wi[0]=e,n}function ss(){return wi[0]}var Zn={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function An(e){if(as[0])throw new Error("");let n=wi[0];if(n===null)return;n.consumerOnSignalRead(e);let t=n.producersTail;if(t!==void 0&&t.producer===e)return;let r,o=n.recomputing;if(o&&(r=t!==void 0?t.nextProducer:n.producers,r!==void 0&&r.producer===e)){n.producersTail=r,r.lastReadVersion=e.version,r.knownValidAtEpoch=wr;return}let i=e.consumersTail;if(i!==void 0&&i.consumer===n&&(!o||i.knownValidAtEpoch===wr))return;let a=yo(n),s={producer:e,consumer:n,nextProducer:r,prevConsumer:void 0,knownValidAtEpoch:wr,lastReadVersion:e.version,nextConsumer:void 0};n.producersTail=s,t!==void 0?t.nextProducer=s:n.producers=s,a&&lh(e,s)}function ah(){wr++}function Yn(e){if(!(yo(e)&&!e.dirty)&&!(!e.dirty&&e.lastCleanEpoch===wr)){if(!e.producerMustRecompute(e)&&!bo(e)){go(e);return}e.producerRecomputeValue(e),go(e)}}function bd(e){if(e.consumers===void 0)return;let n=as[0];as[0]=!0;try{for(let t=e.consumers;t!==void 0;t=t.nextConsumer){let r=t.consumer;r.dirty||rC(r)}}finally{as[0]=n}}function yd(){return wi[0]?.consumerAllowSignalWrites!==!1}function rC(e){e.dirty=!0,bd(e),e.consumerMarkedDirty?.(e)}function go(e){e.dirty=!1,e.lastCleanEpoch=wr}function Rn(e){return e&&sh(e),k(e)}function sh(e){if(e.producersTail?.knownValidAtEpoch===wr){let n=e.producers;for(;n!==void 0;)n.knownValidAtEpoch=null,n=n.nextProducer}e.producersTail=void 0,e.recomputing=!0}function Qn(e,n){k(n),e&&ch(e)}function ch(e){e.recomputing=!1;let n=e.producersTail,t=n!==void 0?n.nextProducer:e.producers;if(t!==void 0){if(yo(e))do t=vd(t);while(t!==void 0);n!==void 0?n.nextProducer=void 0:e.producers=void 0}}function bo(e){for(let n=e.producers;n!==void 0;n=n.nextProducer){let t=n.producer,r=n.lastReadVersion;if(r!==t.version||(Yn(t),r!==t.version))return!0}return!1}function Xn(e){if(yo(e)){let n=e.producers;for(;n!==void 0;)n=vd(n)}e.producers=void 0,e.producersTail=void 0,e.consumers=void 0,e.consumersTail=void 0}function lh(e,n){let t=e.consumersTail,r=yo(e);if(t!==void 0?(n.nextConsumer=t.nextConsumer,t.nextConsumer=n):(n.nextConsumer=void 0,e.consumers=n),n.prevConsumer=t,e.consumersTail=n,!r)for(let o=e.producers;o!==void 0;o=o.nextProducer)lh(o.producer,o)}function vd(e){let n=e.producer,t=e.nextProducer,r=e.nextConsumer,o=e.prevConsumer;if(e.nextConsumer=void 0,e.prevConsumer=void 0,r!==void 0?r.prevConsumer=o:n.consumersTail=o,o!==void 0)o.nextConsumer=r;else if(n.consumers=r,!yo(n)){let i=n.producers;for(;i!==void 0;)i=vd(i)}return t}function yo(e){return e.consumerIsAlwaysLive||e.consumers!==void 0}function Ei(e){nC?.(e)}function Di(e,n){return Object.is(e,n)}function Ii(e,n){let t=Object.create(oC);t.computation=e,n!==void 0&&(t.equal=n);let r=()=>{if(Yn(t),An(t),t.value===Rt)throw t.error;return t.value};return r[Be]=t,Ei(t),r}var Er=Symbol("UNSET"),Dr=Symbol("COMPUTING"),Rt=Symbol("ERRORED"),oC=T(v({},Zn),{value:Er,dirty:!0,error:null,equal:Di,kind:"computed",producerMustRecompute(e){return e.value===Er||e.value===Dr},producerRecomputeValue(e){if(e.value===Dr)throw new Error("");let n=e.value;e.value=Dr;let t=Rn(e),r,o=!1;try{r=e.computation(),k(null),o=n!==Er&&n!==Rt&&r!==Rt&&e.equal(n,r),e.error=null}catch(i){r=Rt,e.error=i}finally{Qn(e,t)}if(o){e.value=n;return}e.value=r,e.version++}});function iC(){throw new Error}var dh=iC;function uh(e){dh(e)}function _d(e){dh=e}var aC=null;function xd(e,n){let t=Object.create(vo);t.value=e,n!==void 0&&(t.equal=n);let r=()=>fh(t);return r[Be]=t,Ei(t),[r,a=>Jn(t,a),a=>cs(t,a)]}function fh(e){return An(e),e.value}function Jn(e,n){yd()||uh(e),e.equal(e.value,n)||(e.value=n,sC(e))}function cs(e,n){yd()||uh(e),Jn(e,n(e.value))}var vo=T(v({},Zn),{equal:Di,value:void 0,kind:"signal"});function sC(e){e.version++,ah(),bd(e),aC?.(e)}var Cd=T(v({},Zn),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Sd(e){if(e.dirty=!1,e.version>0&&!bo(e))return;e.version++;let n=Rn(e);try{e.cleanup(),e.fn()}finally{Qn(e,n)}}var wd=[void 0];function ls(){return wd[0]}function hn(e){let n=wd[0];return wd[0]=e,n}var mh=Symbol("NotFound");function _o(e){return e===mh||e?.name==="\u0275NotFound"}function Ed(e,n,t){let r=Object.create(cC);r.source=e,r.computation=n,t!=null&&(r.equal=t);let i=()=>{if(Yn(r),An(r),r.value===Rt)throw r.error;return r.value};return i[Be]=r,Ei(r),i}function Dd(e,n){Yn(e),Jn(e,n),go(e),e.error=null}function ph(e,n){if(Yn(e),e.value===Rt)throw e.error;cs(e,n),go(e)}var cC=T(v({},Zn),{value:Er,dirty:!0,error:null,equal:Di,kind:"linkedSignal",producerMustRecompute(e){return e.value===Er||e.value===Dr},producerRecomputeValue(e){if(e.value===Dr)throw new Error("");let n=e.value;e.value=Dr;let t=Rn(e),r,o=!1;try{let i=e.source(),a=n!==Er&&n!==Rt,s=a?{source:e.sourceValue,value:n}:void 0;r=e.computation(i,s),e.sourceValue=i,k(null),o=a&&r!==Rt&&e.equal(n,r),e.error=null}catch(i){r=Rt,e.error=i}finally{Qn(e,t)}if(o){e.value=n;return}e.value=r,e.version++}});function hh(e){let n=k(null);try{return e()}finally{k(n)}}function $(e){return typeof e=="function"}function xo(e){let t=e(r=>{Error.call(r),r.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var ds=xo(e=>function(t){e(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((r,o)=>`${o+1}) ${r.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function ki(e,n){if(e){let t=e.indexOf(n);0<=t&&e.splice(t,1)}}var Oe=class e{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let i of t)i.remove(this);else t.remove(this);let{initialTeardown:r}=this;if($(r))try{r()}catch(i){n=i instanceof ds?i.errors:[i]}let{_finalizers:o}=this;if(o){this._finalizers=null;for(let i of o)try{gh(i)}catch(a){n=n??[],a instanceof ds?n=[...n,...a.errors]:n.push(a)}}if(n)throw new ds(n)}}add(n){var t;if(n&&n!==this)if(this.closed)gh(n);else{if(n instanceof e){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(n)}}_hasParent(n){let{_parentage:t}=this;return t===n||Array.isArray(t)&&t.includes(n)}_addParent(n){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(n),t):t?[t,n]:n}_removeParent(n){let{_parentage:t}=this;t===n?this._parentage=null:Array.isArray(t)&&ki(t,n)}remove(n){let{_finalizers:t}=this;t&&ki(t,n),n instanceof e&&n._removeParent(this)}};Oe.EMPTY=(()=>{let e=new Oe;return e.closed=!0,e})();var Id=Oe.EMPTY;function us(e){return e instanceof Oe||e&&"closed"in e&&$(e.remove)&&$(e.add)&&$(e.unsubscribe)}function gh(e){$(e)?e():e.unsubscribe()}var Xt={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Co={setTimeout(e,n,...t){let{delegate:r}=Co;return r?.setTimeout?r.setTimeout(e,n,...t):setTimeout(e,n,...t)},clearTimeout(e){let{delegate:n}=Co;return(n?.clearTimeout||clearTimeout)(e)},delegate:void 0};function fs(e){Co.setTimeout(()=>{let{onUnhandledError:n}=Xt;if(n)n(e);else throw e})}function Mi(){}var bh=kd("C",void 0,void 0);function yh(e){return kd("E",void 0,e)}function vh(e){return kd("N",e,void 0)}function kd(e,n,t){return{kind:e,value:n,error:t}}var Ir=null;function So(e){if(Xt.useDeprecatedSynchronousErrorHandling){let n=!Ir;if(n&&(Ir={errorThrown:!1,error:null}),e(),n){let{errorThrown:t,error:r}=Ir;if(Ir=null,t)throw r}}else e()}function _h(e){Xt.useDeprecatedSynchronousErrorHandling&&Ir&&(Ir.errorThrown=!0,Ir.error=e)}var kr=class extends Oe{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,us(n)&&n.add(this)):this.destination=uC}static create(n,t,r){return new On(n,t,r)}next(n){this.isStopped?Nd(vh(n),this):this._next(n)}error(n){this.isStopped?Nd(yh(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?Nd(bh,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},lC=Function.prototype.bind;function Md(e,n){return lC.call(e,n)}var Td=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:t}=this;if(t.next)try{t.next(n)}catch(r){ms(r)}}error(n){let{partialObserver:t}=this;if(t.error)try{t.error(n)}catch(r){ms(r)}else ms(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(t){ms(t)}}},On=class extends kr{constructor(n,t,r){super();let o;if($(n)||!n)o={next:n??void 0,error:t??void 0,complete:r??void 0};else{let i;this&&Xt.useDeprecatedNextContext?(i=Object.create(n),i.unsubscribe=()=>this.unsubscribe(),o={next:n.next&&Md(n.next,i),error:n.error&&Md(n.error,i),complete:n.complete&&Md(n.complete,i)}):o=n}this.destination=new Td(o)}};function ms(e){Xt.useDeprecatedSynchronousErrorHandling?_h(e):fs(e)}function dC(e){throw e}function Nd(e,n){let{onStoppedNotification:t}=Xt;t&&Co.setTimeout(()=>t(e,n))}var uC={closed:!0,next:Mi,error:dC,complete:Mi};var wo=typeof Symbol=="function"&&Symbol.observable||"@@observable";function wt(e){return e}function ps(...e){return Ad(e)}function Ad(e){return e.length===0?wt:e.length===1?e[0]:function(t){return e.reduce((r,o)=>o(r),t)}}var Z=class e{constructor(n){n&&(this._subscribe=n)}lift(n){let t=new e;return t.source=this,t.operator=n,t}subscribe(n,t,r){let o=mC(n)?n:new On(n,t,r);return So(()=>{let{operator:i,source:a}=this;o.add(i?i.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(n){try{return this._subscribe(n)}catch(t){n.error(t)}}forEach(n,t){return t=xh(t),new t((r,o)=>{let i=new On({next:a=>{try{n(a)}catch(s){o(s),i.unsubscribe()}},error:o,complete:r});this.subscribe(i)})}_subscribe(n){var t;return(t=this.source)===null||t===void 0?void 0:t.subscribe(n)}[wo](){return this}pipe(...n){return Ad(n)(this)}toPromise(n){return n=xh(n),new n((t,r)=>{let o;this.subscribe(i=>o=i,i=>r(i),()=>t(o))})}};Z.create=e=>new Z(e);function xh(e){var n;return(n=e??Xt.Promise)!==null&&n!==void 0?n:Promise}function fC(e){return e&&$(e.next)&&$(e.error)&&$(e.complete)}function mC(e){return e&&e instanceof kr||fC(e)&&us(e)}function pC(e){return $(e?.lift)}function ee(e){return n=>{if(pC(n))return n.lift(function(t){try{return e(t,this)}catch(r){this.error(r)}});throw new TypeError("Unable to lift unknown Observable type")}}function ne(e,n,t,r,o){return new Rd(e,n,t,r,o)}var Rd=class extends kr{constructor(n,t,r,o,i,a){super(n),this.onFinalize=i,this.shouldUnsubscribe=a,this._next=t?function(s){try{t(s)}catch(c){n.error(c)}}:super._next,this._error=o?function(s){try{o(s)}catch(c){n.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=r?function(){try{r()}catch(s){n.error(s)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var Ch=xo(e=>function(){e(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var q=class extends Z{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(n){let t=new hs(this,this);return t.operator=n,t}_throwIfClosed(){if(this.closed)throw new Ch}next(n){So(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let t of this.currentObservers)t.next(n)}})}error(n){So(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=n;let{observers:t}=this;for(;t.length;)t.shift().error(n)}})}complete(){So(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:n}=this;for(;n.length;)n.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var n;return((n=this.observers)===null||n===void 0?void 0:n.length)>0}_trySubscribe(n){return this._throwIfClosed(),super._trySubscribe(n)}_subscribe(n){return this._throwIfClosed(),this._checkFinalizedStatuses(n),this._innerSubscribe(n)}_innerSubscribe(n){let{hasError:t,isStopped:r,observers:o}=this;return t||r?Id:(this.currentObservers=null,o.push(n),new Oe(()=>{this.currentObservers=null,ki(o,n)}))}_checkFinalizedStatuses(n){let{hasError:t,thrownError:r,isStopped:o}=this;t?n.error(r):o&&n.complete()}asObservable(){let n=new Z;return n.source=this,n}};q.create=(e,n)=>new hs(e,n);var hs=class extends q{constructor(n,t){super(),this.destination=n,this.source=t}next(n){var t,r;(r=(t=this.destination)===null||t===void 0?void 0:t.next)===null||r===void 0||r.call(t,n)}error(n){var t,r;(r=(t=this.destination)===null||t===void 0?void 0:t.error)===null||r===void 0||r.call(t,n)}complete(){var n,t;(t=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||t===void 0||t.call(n)}_subscribe(n){var t,r;return(r=(t=this.source)===null||t===void 0?void 0:t.subscribe(n))!==null&&r!==void 0?r:Id}};var ze=class extends q{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let t=super._subscribe(n);return!t.closed&&n.next(this._value),t}getValue(){let{hasError:n,thrownError:t,_value:r}=this;if(n)throw t;return this._throwIfClosed(),r}next(n){super.next(this._value=n)}};var Od={now(){return(Od.delegate||Date).now()},delegate:void 0};var gs=class extends q{constructor(n=1/0,t=1/0,r=Od){super(),this._bufferSize=n,this._windowTime=t,this._timestampProvider=r,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=t===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,t)}next(n){let{isStopped:t,_buffer:r,_infiniteTimeWindow:o,_timestampProvider:i,_windowTime:a}=this;t||(r.push(n),!o&&r.push(i.now()+a)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let t=this._innerSubscribe(n),{_infiniteTimeWindow:r,_buffer:o}=this,i=o.slice();for(let a=0;a<i.length&&!n.closed;a+=r?1:2)n.next(i[a]);return this._checkFinalizedStatuses(n),t}_trimBuffer(){let{_bufferSize:n,_timestampProvider:t,_buffer:r,_infiniteTimeWindow:o}=this,i=(o?1:2)*n;if(n<1/0&&i<r.length&&r.splice(0,r.length-i),!o){let a=t.now(),s=0;for(let c=1;c<r.length&&r[c]<=a;c+=2)s=c;s&&r.splice(0,s+1)}}};var Fe=new Z(e=>e.complete());function Sh(e){return e&&$(e.schedule)}function Fd(e){return e[e.length-1]}function bs(e){return $(Fd(e))?e.pop():void 0}function gn(e){return Sh(Fd(e))?e.pop():void 0}function wh(e,n){return typeof Fd(e)=="number"?e.pop():n}function Dh(e,n,t,r){function o(i){return i instanceof t?i:new t(function(a){a(i)})}return new(t||(t=Promise))(function(i,a){function s(d){try{l(r.next(d))}catch(u){a(u)}}function c(d){try{l(r.throw(d))}catch(u){a(u)}}function l(d){d.done?i(d.value):o(d.value).then(s,c)}l((r=r.apply(e,n||[])).next())})}function Eh(e){var n=typeof Symbol=="function"&&Symbol.iterator,t=n&&e[n],r=0;if(t)return t.call(e);if(e&&typeof e.length=="number")return{next:function(){return e&&r>=e.length&&(e=void 0),{value:e&&e[r++],done:!e}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function Mr(e){return this instanceof Mr?(this.v=e,this):new Mr(e)}function Ih(e,n,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var r=t.apply(e,n||[]),o,i=[];return o=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),s("next"),s("throw"),s("return",a),o[Symbol.asyncIterator]=function(){return this},o;function a(h){return function(_){return Promise.resolve(_).then(h,u)}}function s(h,_){r[h]&&(o[h]=function(D){return new Promise(function(A,j){i.push([h,D,A,j])>1||c(h,D)})},_&&(o[h]=_(o[h])))}function c(h,_){try{l(r[h](_))}catch(D){g(i[0][3],D)}}function l(h){h.value instanceof Mr?Promise.resolve(h.value.v).then(d,u):g(i[0][2],h)}function d(h){c("next",h)}function u(h){c("throw",h)}function g(h,_){h(_),i.shift(),i.length&&c(i[0][0],i[0][1])}}function kh(e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=e[Symbol.asyncIterator],t;return n?n.call(e):(e=typeof Eh=="function"?Eh(e):e[Symbol.iterator](),t={},r("next"),r("throw"),r("return"),t[Symbol.asyncIterator]=function(){return this},t);function r(i){t[i]=e[i]&&function(a){return new Promise(function(s,c){a=e[i](a),o(s,c,a.done,a.value)})}}function o(i,a,s,c){Promise.resolve(c).then(function(l){i({value:l,done:s})},a)}}var ys=(e=>e&&typeof e.length=="number"&&typeof e!="function");function vs(e){return $(e?.then)}function _s(e){return $(e[wo])}function xs(e){return Symbol.asyncIterator&&$(e?.[Symbol.asyncIterator])}function Cs(e){return new TypeError(`You provided ${e!==null&&typeof e=="object"?"an invalid object":`'${e}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function hC(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Ss=hC();function ws(e){return $(e?.[Ss])}function Es(e){return Ih(this,arguments,function*(){let t=e.getReader();try{for(;;){let{value:r,done:o}=yield Mr(t.read());if(o)return yield Mr(void 0);yield yield Mr(r)}}finally{t.releaseLock()}})}function Ds(e){return $(e?.getReader)}function Se(e){if(e instanceof Z)return e;if(e!=null){if(_s(e))return gC(e);if(ys(e))return bC(e);if(vs(e))return yC(e);if(xs(e))return Mh(e);if(ws(e))return vC(e);if(Ds(e))return _C(e)}throw Cs(e)}function gC(e){return new Z(n=>{let t=e[wo]();if($(t.subscribe))return t.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function bC(e){return new Z(n=>{for(let t=0;t<e.length&&!n.closed;t++)n.next(e[t]);n.complete()})}function yC(e){return new Z(n=>{e.then(t=>{n.closed||(n.next(t),n.complete())},t=>n.error(t)).then(null,fs)})}function vC(e){return new Z(n=>{for(let t of e)if(n.next(t),n.closed)return;n.complete()})}function Mh(e){return new Z(n=>{xC(e,n).catch(t=>n.error(t))})}function _C(e){return Mh(Es(e))}function xC(e,n){var t,r,o,i;return Dh(this,void 0,void 0,function*(){try{for(t=kh(e);r=yield t.next(),!r.done;){let a=r.value;if(n.next(a),n.closed)return}}catch(a){o={error:a}}finally{try{r&&!r.done&&(i=t.return)&&(yield i.call(t))}finally{if(o)throw o.error}}n.complete()})}function gt(e,n,t,r=0,o=!1){let i=n.schedule(function(){t(),o?e.add(this.schedule(null,r)):this.unsubscribe()},r);if(e.add(i),!o)return i}function Is(e,n=0){return ee((t,r)=>{t.subscribe(ne(r,o=>gt(r,e,()=>r.next(o),n),()=>gt(r,e,()=>r.complete(),n),o=>gt(r,e,()=>r.error(o),n)))})}function ks(e,n=0){return ee((t,r)=>{r.add(e.schedule(()=>t.subscribe(r),n))})}function Nh(e,n){return Se(e).pipe(ks(n),Is(n))}function Th(e,n){return Se(e).pipe(ks(n),Is(n))}function Ah(e,n){return new Z(t=>{let r=0;return n.schedule(function(){r===e.length?t.complete():(t.next(e[r++]),t.closed||this.schedule())})})}function Rh(e,n){return new Z(t=>{let r;return gt(t,n,()=>{r=e[Ss](),gt(t,n,()=>{let o,i;try{({value:o,done:i}=r.next())}catch(a){t.error(a);return}i?t.complete():t.next(o)},0,!0)}),()=>$(r?.return)&&r.return()})}function Ms(e,n){if(!e)throw new Error("Iterable cannot be null");return new Z(t=>{gt(t,n,()=>{let r=e[Symbol.asyncIterator]();gt(t,n,()=>{r.next().then(o=>{o.done?t.complete():t.next(o.value)})},0,!0)})})}function Oh(e,n){return Ms(Es(e),n)}function Fh(e,n){if(e!=null){if(_s(e))return Nh(e,n);if(ys(e))return Ah(e,n);if(vs(e))return Th(e,n);if(xs(e))return Ms(e,n);if(ws(e))return Rh(e,n);if(Ds(e))return Oh(e,n)}throw Cs(e)}function we(e,n){return n?Fh(e,n):Se(e)}function X(...e){let n=gn(e);return we(e,n)}function Pd(e,n){let t=$(e)?e:()=>e,r=o=>o.error(t());return new Z(n?o=>n.schedule(r,0,o):r)}function Ns(e){return!!e&&(e instanceof Z||$(e.lift)&&$(e.subscribe))}var Nr=xo(e=>function(){e(this),this.name="EmptyError",this.message="no elements in sequence"});function be(e,n){return ee((t,r)=>{let o=0;t.subscribe(ne(r,i=>{r.next(e.call(n,i,o++))}))})}var{isArray:CC}=Array;function SC(e,n){return CC(n)?e(...n):e(n)}function Ts(e){return be(n=>SC(e,n))}var{isArray:wC}=Array,{getPrototypeOf:EC,prototype:DC,keys:IC}=Object;function As(e){if(e.length===1){let n=e[0];if(wC(n))return{args:n,keys:null};if(kC(n)){let t=IC(n);return{args:t.map(r=>n[r]),keys:t}}}return{args:e,keys:null}}function kC(e){return e&&typeof e=="object"&&EC(e)===DC}function Rs(e,n){return e.reduce((t,r,o)=>(t[r]=n[o],t),{})}function Ld(...e){let n=gn(e),t=bs(e),{args:r,keys:o}=As(e);if(r.length===0)return we([],n);let i=new Z(MC(r,n,o?a=>Rs(o,a):wt));return t?i.pipe(Ts(t)):i}function MC(e,n,t=wt){return r=>{Ph(n,()=>{let{length:o}=e,i=new Array(o),a=o,s=o;for(let c=0;c<o;c++)Ph(n,()=>{let l=we(e[c],n),d=!1;l.subscribe(ne(r,u=>{i[c]=u,d||(d=!0,s--),s||r.next(t(i.slice()))},()=>{--a||r.complete()}))},r)},r)}}function Ph(e,n,t){e?gt(t,e,n):n()}function Lh(e,n,t,r,o,i,a,s){let c=[],l=0,d=0,u=!1,g=()=>{u&&!c.length&&!l&&n.complete()},h=D=>l<r?_(D):c.push(D),_=D=>{i&&n.next(D),l++;let A=!1;Se(t(D,d++)).subscribe(ne(n,j=>{o?.(j),i?h(j):n.next(j)},()=>{A=!0},void 0,()=>{if(A)try{for(l--;c.length&&l<r;){let j=c.shift();a?gt(n,a,()=>_(j)):_(j)}g()}catch(j){n.error(j)}}))};return e.subscribe(ne(n,h,()=>{u=!0,g()})),()=>{s?.()}}function lt(e,n,t=1/0){return $(n)?lt((r,o)=>be((i,a)=>n(r,i,o,a))(Se(e(r,o))),t):(typeof n=="number"&&(t=n),ee((r,o)=>Lh(r,o,e,t)))}function Os(e=1/0){return lt(wt,e)}function Vh(){return Os(1)}function Eo(...e){return Vh()(we(e,gn(e)))}function Ni(e){return new Z(n=>{Se(e()).subscribe(n)})}function Vd(...e){let n=bs(e),{args:t,keys:r}=As(e),o=new Z(i=>{let{length:a}=t;if(!a){i.complete();return}let s=new Array(a),c=a,l=a;for(let d=0;d<a;d++){let u=!1;Se(t[d]).subscribe(ne(i,g=>{u||(u=!0,l--),s[d]=g},()=>c--,void 0,()=>{(!c||!u)&&(l||i.next(r?Rs(r,s):s),i.complete())}))}});return n?o.pipe(Ts(n)):o}function jd(...e){let n=gn(e),t=wh(e,1/0),r=e;return r.length?r.length===1?Se(r[0]):Os(t)(we(r,n)):Fe}function dt(e,n){return ee((t,r)=>{let o=0;t.subscribe(ne(r,i=>e.call(n,i,o++)&&r.next(i)))})}function Do(e){return ee((n,t)=>{let r=null,o=!1,i;r=n.subscribe(ne(t,void 0,void 0,a=>{i=Se(e(a,Do(e)(n))),r?(r.unsubscribe(),r=null,i.subscribe(t)):o=!0})),o&&(r.unsubscribe(),r=null,i.subscribe(t))})}function Fs(e,n){return $(n)?lt(e,n,1):lt(e,1)}function jh(e){return ee((n,t)=>{let r=!1;n.subscribe(ne(t,o=>{r=!0,t.next(o)},()=>{r||t.next(e),t.complete()}))})}function er(e){return e<=0?()=>Fe:ee((n,t)=>{let r=0;n.subscribe(ne(t,o=>{++r<=e&&(t.next(o),e<=r&&t.complete())}))})}function Bd(e,n=wt){return e=e??NC,ee((t,r)=>{let o,i=!0;t.subscribe(ne(r,a=>{let s=n(a);(i||!e(o,s))&&(i=!1,o=s,r.next(a))}))})}function NC(e,n){return e===n}function Bh(e=TC){return ee((n,t)=>{let r=!1;n.subscribe(ne(t,o=>{r=!0,t.next(o)},()=>r?t.complete():t.error(e())))})}function TC(){return new Nr}function Ud(e){return ee((n,t)=>{try{n.subscribe(t)}finally{t.add(e)}})}function Fn(e,n){let t=arguments.length>=2;return r=>r.pipe(e?dt((o,i)=>e(o,i,r)):wt,er(1),t?jh(n):Bh(()=>new Nr))}function Ps(e){return e<=0?()=>Fe:ee((n,t)=>{let r=[];n.subscribe(ne(t,o=>{r.push(o),e<r.length&&r.shift()},()=>{for(let o of r)t.next(o);t.complete()},void 0,()=>{r=null}))})}function Hd(){return ee((e,n)=>{let t,r=!1;e.subscribe(ne(n,o=>{let i=t;t=o,r&&n.next([i,o]),r=!0}))})}function Uh(e={}){let{connector:n=()=>new q,resetOnError:t=!0,resetOnComplete:r=!0,resetOnRefCountZero:o=!0}=e;return i=>{let a,s,c,l=0,d=!1,u=!1,g=()=>{s?.unsubscribe(),s=void 0},h=()=>{g(),a=c=void 0,d=u=!1},_=()=>{let D=a;h(),D?.unsubscribe()};return ee((D,A)=>{l++,!u&&!d&&g();let j=c=c??n();A.add(()=>{l--,l===0&&!u&&!d&&(s=zd(_,o))}),j.subscribe(A),!a&&l>0&&(a=new On({next:J=>j.next(J),error:J=>{u=!0,g(),s=zd(h,t,J),j.error(J)},complete:()=>{d=!0,g(),s=zd(h,r),j.complete()}}),Se(D).subscribe(a))})(i)}}function zd(e,n,...t){if(n===!0){e();return}if(n===!1)return;let r=new On({next:()=>{r.unsubscribe(),e()}});return Se(n(...t)).subscribe(r)}function $d(e,n,t){let r,o=!1;return e&&typeof e=="object"?{bufferSize:r=1/0,windowTime:n=1/0,refCount:o=!1,scheduler:t}=e:r=e??1/0,Uh({connector:()=>new gs(r,n,t),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:o})}function Gd(e){return dt((n,t)=>e<=t)}function Ti(...e){let n=gn(e);return ee((t,r)=>{(n?Eo(e,t,n):Eo(e,t)).subscribe(r)})}function Ot(e,n){return ee((t,r)=>{let o=null,i=0,a=!1,s=()=>a&&!o&&r.complete();t.subscribe(ne(r,c=>{o?.unsubscribe();let l=0,d=i++;Se(e(c,d)).subscribe(o=ne(r,u=>r.next(n?n(c,u,d,l++):u),()=>{o=null,s()}))},()=>{a=!0,s()}))})}function Jt(e){return ee((n,t)=>{Se(e).subscribe(ne(t,()=>t.complete(),Mi)),!t.closed&&n.subscribe(t)})}function Ft(e,n,t){let r=$(e)||n||t?{next:e,error:n,complete:t}:e;return r?ee((o,i)=>{var a;(a=r.subscribe)===null||a===void 0||a.call(r);let s=!0;o.subscribe(ne(i,c=>{var l;(l=r.next)===null||l===void 0||l.call(r,c),i.next(c)},()=>{var c;s=!1,(c=r.complete)===null||c===void 0||c.call(r),i.complete()},c=>{var l;s=!1,(l=r.error)===null||l===void 0||l.call(r,c),i.error(c)},()=>{var c,l;s&&((c=r.unsubscribe)===null||c===void 0||c.call(r)),(l=r.finalize)===null||l===void 0||l.call(r)}))}):wt}var su="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",I=class extends Error{code;constructor(n,t){super(nr(n,t)),this.code=n}};function AC(e){return`NG0${Math.abs(e)}`}function nr(e,n){return`${AC(e)}${n?": "+n:""}`}function pe(e){for(let n in e)if(e[n]===pe)return n;throw Error("")}function qh(e,n){for(let t in n)Object.hasOwn(n,t)&&!Object.hasOwn(e,t)&&(e[t]=n[t])}function Li(e){if(typeof e=="string")return e;if(Array.isArray(e))return`[${e.map(Li).join(", ")}]`;if(e==null)return""+e;let n=e.overriddenName||e.name;if(n)return`${n}`;let t=e.toString();if(t==null)return""+t;let r=t.indexOf(`
`);return r>=0?t.slice(0,r):t}function zs(e,n){return e?n?`${e} ${n}`:e:n||""}var RC=pe({__forward_ref__:pe});function bt(e){return e.__forward_ref__=bt,e}function qe(e){return cu(e)?e():e}function cu(e){return typeof e=="function"&&Object.hasOwn(e,RC)&&e.__forward_ref__===bt}function G(e){return{token:e.token,providedIn:e.providedIn||null,factory:e.factory,value:void 0}}function Ee(e){return{providers:e.providers||[],imports:e.imports||[]}}function Vi(e){return OC(e,$s)}function lu(e){return Vi(e)!==null}function OC(e,n){return Object.hasOwn(e,n)&&e[n]||null}function FC(e){let n=e?.[$s]??null;return n||null}function qd(e){return e&&Object.hasOwn(e,Vs)?e[Vs]:null}var $s=pe({\u0275prov:pe}),Vs=pe({\u0275inj:pe}),C=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,t){this._desc=n,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=G({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function du(e){return e&&!!e.\u0275providers}var ji=pe({\u0275cmp:pe}),Bi=pe({\u0275dir:pe}),uu=pe({\u0275pipe:pe}),fu=pe({\u0275mod:pe}),Oi=pe({\u0275fac:pe}),Lr=pe({__NG_ELEMENT_ID__:pe}),Hh=pe({__NG_ENV_ID__:pe});function Kh(e){return Ws(e,"@NgModule"),e[fu]||null}function rr(e){return Ws(e,"@Component"),e[ji]||null}function Gs(e){return Ws(e,"@Directive"),e[Bi]||null}function Zh(e){return Ws(e,"@Pipe"),e[uu]||null}function Ws(e,n){if(e==null)throw new I(-919,!1)}function bn(e){return typeof e=="string"?e:e==null?"":String(e)}var Yh=pe({ngErrorCode:pe}),PC=pe({ngErrorMessage:pe}),LC=pe({ngTokenPath:pe});function mu(e,n){return Qh("",-200,n)}function qs(e,n){throw new I(-201,!1)}function Qh(e,n,t){let r=new I(n,e);return r[Yh]=n,r[PC]=e,t&&(r[LC]=t),r}function VC(e){return e[Yh]}var Kd;function Xh(){return Kd}function Et(e){let n=Kd;return Kd=e,n}function pu(e,n,t){let r=Vi(e);if(r&&r.providedIn=="root")return r.value===void 0?r.value=r.factory():r.value;if(t&8)return null;if(n!==void 0)return n;qs(e,"")}var Ui=globalThis;var jC={},Tr=jC,BC="__NG_DI_FLAG__",Zd=class{injector;constructor(n){this.injector=n}retrieve(n,t){let r=Ar(t)||0;try{return this.injector.get(n,r&8?null:Tr,r)}catch(o){if(_o(o))return o;throw o}}};function UC(e,n=0){let t=ls();if(t===void 0)throw new I(-203,!1);if(t===null)return pu(e,void 0,n);{let r=HC(n),o=t.retrieve(e,r);if(_o(o)){if(r.optional)return null;throw o}return o}}function H(e,n=0){return(Xh()||UC)(qe(e),n)}function f(e,n){return H(e,Ar(n))}function Ar(e){return typeof e>"u"||typeof e=="number"?e:0|(e.optional&&8)|(e.host&&1)|(e.self&&2)|(e.skipSelf&&4)}function HC(e){return{optional:!!(e&8),host:!!(e&1),self:!!(e&2),skipSelf:!!(e&4)}}function Yd(e){let n=[];for(let t=0;t<e.length;t++){let r=qe(e[t]);if(Array.isArray(r)){if(r.length===0)throw new I(900,!1);let o,i=0;for(let a=0;a<r.length;a++){let s=r[a],c=zC(s);typeof c=="number"?c===-1?o=s.token:i|=c:o=s}n.push(H(o,i))}else n.push(H(r))}return n}function zC(e){return e[BC]}function Rr(e,n){let t=Object.hasOwn(e,Oi);return t?e[Oi]:null}function Jh(e,n,t){if(e.length!==n.length)return!1;for(let r=0;r<e.length;r++){let o=e[r],i=n[r];if(t&&(o=t(o),i=t(i)),i!==o)return!1}return!0}function eg(e){return e.flat(Number.POSITIVE_INFINITY)}function Ks(e,n){e.forEach(t=>Array.isArray(t)?Ks(t,n):n(t))}function hu(e,n,t){n>=e.length?e.push(t):e.splice(n,0,t)}function Hi(e,n){return n>=e.length-1?e.pop():e.splice(n,1)[0]}function tg(e,n){let t=[];for(let r=0;r<e;r++)t.push(n);return t}function ng(e,n,t,r){let o=e.length;if(o==n)e.push(t,r);else if(o===1)e.push(r,e[0]),e[0]=t;else{for(o--,e.push(e[o-1],e[o]);o>n;){let i=o-2;e[o]=e[i],o--}e[n]=t,e[n+1]=r}}function Zs(e,n,t){let r=Mo(e,n);return r>=0?e[r|1]=t:(r=~r,ng(e,r,n,t)),r}function Ys(e,n){let t=Mo(e,n);if(t>=0)return e[t|1]}function Mo(e,n){return $C(e,n,1)}function $C(e,n,t){let r=0,o=e.length>>t;for(;o!==r;){let i=r+(o-r>>1),a=e[i<<t];if(n===a)return i<<t;a>n?o=i:r=i+1}return~(o<<t)}var or={},et=[],No=new C(""),gu=new C("",-1),bu=new C(""),ko=class{get(n,t=Tr){if(t===Tr){let o=Qh("",-201);throw o.name="\u0275NotFound",o}return t}};function ir(e){return{\u0275providers:e}}function rg(e){return ir([{provide:No,multi:!0,useValue:e}])}function og(...e){return{\u0275providers:yu(!0,e),\u0275fromNgModule:!0}}function yu(e,...n){let t=[],r=new Set,o,i=a=>{t.push(a)};return Ks(n,a=>{let s=a;js(s,i,[],r)&&(o||=[],o.push(s))}),o!==void 0&&ig(o,i),t}function ig(e,n){for(let t=0;t<e.length;t++){let{ngModule:r,providers:o}=e[t];vu(o,i=>{n(i,r)})}}function js(e,n,t,r){if(e=qe(e),!e)return!1;let o=null,i=qd(e),a=!i&&rr(e);if(!i&&!a){let c=e.ngModule;if(i=qd(c),i)o=c;else return!1}else{if(a&&!a.standalone)return!1;o=e}let s=r.has(o);if(a){if(s)return!1;if(r.add(o),a.dependencies){let c=typeof a.dependencies=="function"?a.dependencies():a.dependencies;for(let l of c)js(l,n,t,r)}}else if(i){if(i.imports!=null&&!s){r.add(o);let l;Ks(i.imports,d=>{js(d,n,t,r)&&(l||=[],l.push(d))}),l!==void 0&&ig(l,n)}if(!s){let l=Rr(o)||(()=>new o);n({provide:o,useFactory:l,deps:et},o),n({provide:bu,useValue:o,multi:!0},o),n({provide:No,useValue:()=>H(o),multi:!0},o)}let c=i.providers;if(c!=null&&!s){let l=e;vu(c,d=>{n(d,l)})}}else return!1;return o!==e&&e.providers!==void 0}function vu(e,n){for(let t of e)du(t)&&(t=t.\u0275providers),Array.isArray(t)?vu(t,n):n(t)}var GC=pe({provide:String,useValue:pe});function ag(e){return e!==null&&typeof e=="object"&&GC in e}function WC(e){return!!(e&&e.useExisting)}function qC(e){return!!(e&&e.useFactory)}function Or(e){return typeof e=="function"}function sg(e){return!!e.useClass}var zi=new C(""),Ls={},zh={},Wd;function To(){return Wd===void 0&&(Wd=new ko),Wd}var Pe=class{},Fr=class extends Pe{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,t,r,o){super(),this.parent=t,this.source=r,this.scopes=o,Xd(n,a=>this.processProvider(a)),this.records.set(gu,Io(void 0,this)),o.has("environment")&&this.records.set(Pe,Io(void 0,this));let i=this.records.get(zi);i!=null&&typeof i.value=="string"&&this.scopes.add(i.value),this.injectorDefTypes=new Set(this.get(bu,et,{self:!0}))}retrieve(n,t){let r=Ar(t)||0;try{return this.get(n,Tr,r)}catch(o){if(_o(o))return o;throw o}}destroy(){Ai(this),this._destroyed=!0;let n=k(null);try{for(let r of this._ngOnDestroyHooks)r.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let r of t)r()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),k(n)}}onDestroy(n){return Ai(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){Ai(this);let t=hn(this),r=Et(void 0),o;try{return n()}finally{hn(t),Et(r)}}get(n,t=Tr,r){if(Ai(this),Object.hasOwn(n,Hh))return n[Hh](this);let o=Ar(r),i,a=hn(this),s=Et(void 0);try{if(!(o&4)){let l=this.records.get(n);if(l===void 0){let d=XC(n)&&Vi(n);d&&this.injectableDefInScope(d)?l=Io(Qd(n),Ls):l=null,this.records.set(n,l)}if(l!=null)return this.hydrate(n,l,o)}let c=o&2?To():this.parent;return t=o&8&&t===Tr?null:t,c.get(n,t)}catch(c){let l=VC(c);throw l===-200||l===-201?new I(l,null):c}finally{Et(s),hn(a)}}resolveInjectorInitializers(){let n=k(null),t=hn(this),r=Et(void 0),o;try{let i=this.get(No,et,{self:!0});for(let a of i)a()}finally{hn(t),Et(r),k(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=qe(n);let t=Or(n)?n:qe(n&&n.provide),r=ZC(n);if(!Or(n)&&n.multi===!0){let o=this.records.get(t);o||(o=Io(void 0,Ls,!0),o.factory=()=>Yd(o.multi),this.records.set(t,o)),t=n,o.multi.push(n)}this.records.set(t,r)}hydrate(n,t,r){let o=k(null);try{if(t.value===zh)throw mu("");return t.value===Ls&&(t.value=zh,t.value=t.factory(void 0,r)),typeof t.value=="object"&&t.value&&QC(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{k(o)}}injectableDefInScope(n){if(!n.providedIn)return!1;let t=qe(n.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(n){let t=this._onDestroyHooks.indexOf(n);t!==-1&&this._onDestroyHooks.splice(t,1)}};function Qd(e){let n=Vi(e),t=n!==null?n.factory:Rr(e);if(t!==null)return t;if(e instanceof C)throw new I(-204,!1);if(e instanceof Function)return KC(e);throw new I(-204,!1)}function KC(e){if(e.length>0)throw new I(-204,!1);let t=FC(e);return t!==null?()=>t.factory(e):()=>new e}function ZC(e){if(ag(e))return Io(void 0,e.useValue);{let n=_u(e);return Io(n,Ls)}}function _u(e,n,t){let r;if(Or(e)){let o=qe(e);return Rr(o)||Qd(o)}else if(ag(e))r=()=>qe(e.useValue);else if(qC(e))r=()=>e.useFactory(...Yd(e.deps||[]));else if(WC(e))r=(o,i)=>H(qe(e.useExisting),i!==void 0&&i&8?8:void 0);else{let o=qe(e&&(e.useClass||e.provide));if(YC(e))r=()=>new o(...Yd(e.deps));else return Rr(o)||Qd(o)}return r}function Ai(e){if(e.destroyed)throw new I(-205,!1)}function Io(e,n,t=!1){return{factory:e,value:n,multi:t?[]:void 0}}function YC(e){return!!e.deps}function QC(e){return e!==null&&typeof e=="object"&&typeof e.ngOnDestroy=="function"}function XC(e){return typeof e=="function"||typeof e=="object"&&e.ngMetadataName==="InjectionToken"}function Xd(e,n){for(let t of e)Array.isArray(t)?Xd(t,n):t&&du(t)?Xd(t.\u0275providers,n):n(t)}function Ke(e,n){let t;e instanceof Fr?(Ai(e),t=e):t=new Zd(e);let r,o=hn(t),i=Et(void 0);try{return n()}finally{hn(o),Et(i)}}function cg(){return Xh()!==void 0||ls()!=null}var tn=0,O=1,F=2,Le=3,Pt=4,Ze=5,Vr=6,Ao=7,Me=8,Bn=9,nn=10,De=11,Ro=12,xu=13,ar=14,nt=15,sr=16,jr=17,yn=18,$i=19,Cu=20,Pn=21,Qs=22,Ln=23,Dt=24,Br=25,lg=26,Oo=27,Ve=28,dg=1,Su=6,Ur=7,Gi=8,Hr=9,Ne=10;function Un(e){return Array.isArray(e)&&typeof e[dg]=="object"}function Lt(e){return Array.isArray(e)&&e[dg]===!0}function wu(e){return(e.flags&4)!==0}function Hn(e){return e.componentOffset>-1}function Wi(e){return(e.flags&1)===1}function vn(e){return!!e.template}function Fo(e){return(e[F]&512)!==0}function zr(e){return(e[F]&256)===256}var We=(function(e){return e[e.NONE=0]="NONE",e[e.HTML=1]="HTML",e[e.STYLE=2]="STYLE",e[e.SCRIPT=3]="SCRIPT",e[e.URL=4]="URL",e[e.RESOURCE_URL=5]="RESOURCE_URL",e[e.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",e})(We||{}),Ri,Pr="svg",Xs="math",Jd="",$h="*",eu=()=>Object.create(null);function JC(){return Ri||(Ri=eu(),tr(We.HTML,void 0,[["iframe",["srcdoc"]],["*",["innerHTML","outerHTML"]]]),tr(We.STYLE,void 0,[["*",["style"]]]),tr(We.URL,void 0,[["*",["formAction"]],["area",["href"]],["a",["href","xlink:href"]],["form",["action"]]]),tr(We.URL,Xs,[["*",["href","xlink:href"]]]),tr(We.RESOURCE_URL,void 0,[["base",["href"]],["embed",["src"]],["frame",["src"]],["iframe",["src"]],["link",["href"]],["object",["codebase","data"]]]),tr(We.URL,Pr,[["a",["href","xlink:href"]]]),tr(We.ATTRIBUTE_NO_BINDING,Pr,[["animate",["attributeName","values","to","from"]],["set",["to","attributeName"]],["animateMotion",["attributeName"]],["animateTransform",["attributeName"]]]),tr(We.ATTRIBUTE_NO_BINDING,void 0,[["unknown",["attributeName","values","to","from","sandbox","allow","allowFullscreen","referrerPolicy","csp","fetchPriority","credentialless"]],["iframe",["sandbox","allow","allowFullscreen","referrerPolicy","csp","fetchPriority","credentialless"]]]),Ri)}function tr(e,n,t){let r=n??Jd;for(let[o,i]of t){let a=o.toLowerCase();for(let s of i){let c=s.toLowerCase(),l=Ri[c]??=eu(),d=l[r]??=eu();d[a]=e}}}function ug(e,n,t){let o=JC()[n.toLowerCase()];if(!o)return We.NONE;let i=e.toLowerCase(),a;if(t){let s=o[t];s&&(a=s[i]??s[$h])}if(a===void 0){let s=o[Jd];s&&(a=s[i]??s[$h])}if(a===void 0&&(!t||t===Jd)){let s=o[Pr];s&&(a=s[i])}return a??We.NONE}function Ye(e){for(;Array.isArray(e);)e=e[tn];return e}function Eu(e,n){return Ye(n[e])}function Vt(e,n){return Ye(n[e.index])}function Js(e,n){return e.data[n]}function fg(e,n){return e[n]}function jt(e,n){let t=n[e];return Un(t)?t:t[tn]}function mg(e){return(e[F]&4)===4}function ec(e){return(e[F]&128)===128}function pg(e){return Lt(e[Le])}function Bt(e,n){return n==null?null:e[n]}function Du(e){e[jr]=0}function Iu(e){e[F]&1024||(e[F]|=1024,ec(e)&&$r(e))}function hg(e,n){for(;e>0;)n=n[ar],e--;return n}function qi(e){return!!(e[F]&9216||e[Dt]?.dirty)}function tc(e){e[nn].changeDetectionScheduler?.notify(8),e[F]&64&&(e[F]|=1024),qi(e)&&$r(e)}function $r(e){e[nn].changeDetectionScheduler?.notify(0);let n=Vn(e);for(;n!==null&&!(n[F]&8192||(n[F]|=8192,!ec(n)));)n=Vn(n)}function nc(e,n){if(zr(e))throw new I(911,!1);e[Pn]===null&&(e[Pn]=[]),e[Pn].push(n)}function gg(e,n){if(e[Pn]===null)return;let t=e[Pn].indexOf(n);t!==-1&&e[Pn].splice(t,1)}function Vn(e){let n=e[Le];return Lt(n)?n[Le]:n}function ku(e){return e[Ao]??=[]}function Mu(e){return e.cleanup??=[]}function bg(e,n,t,r){let o=ku(n);o.push(t),e.firstCreatePass&&Mu(e).push(r,o.length-1)}var K={lFrame:kg(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var tu=!1;function yg(){return K.lFrame.elementDepthCount}function vg(){K.lFrame.elementDepthCount++}function Nu(){K.lFrame.elementDepthCount--}function Tu(){return K.bindingsEnabled}function Au(){return K.skipHydrationRootTNode!==null}function Ru(e){return K.skipHydrationRootTNode===e}function Ou(){K.skipHydrationRootTNode=null}function z(){return K.lFrame.lView}function Ue(){return K.lFrame.tView}function P(e){return K.lFrame.contextLView=e,e[Me]}function L(e){return K.lFrame.contextLView=null,e}function rt(){let e=Fu();for(;e!==null&&e.type===64;)e=e.parent;return e}function Fu(){return K.lFrame.currentTNode}function _g(){let e=K.lFrame,n=e.currentTNode;return e.isParent?n:n.parent}function Po(e,n){let t=K.lFrame;t.currentTNode=e,t.isParent=n}function Pu(){return K.lFrame.isParent}function Lu(){K.lFrame.isParent=!1}function xg(){return K.lFrame.contextLView}function Vu(){return tu}function Fi(e){let n=tu;return tu=e,n}function ju(){return K.lFrame.bindingIndex}function Cg(e){return K.lFrame.bindingIndex=e}function Gr(){return K.lFrame.bindingIndex++}function Ki(e){let n=K.lFrame,t=n.bindingIndex;return n.bindingIndex=n.bindingIndex+e,t}function Sg(){return K.lFrame.inI18n}function wg(e,n){let t=K.lFrame;t.bindingIndex=t.bindingRootIndex=e,rc(n)}function Eg(){return K.lFrame.currentDirectiveIndex}function rc(e){K.lFrame.currentDirectiveIndex=e}function Dg(e){let n=K.lFrame.currentDirectiveIndex;return n===-1?null:e[n]}function oc(){return K.lFrame.currentQueryIndex}function Zi(e){K.lFrame.currentQueryIndex=e}function eS(e){let n=e[O];return n.type===2?n.declTNode:n.type===1?e[Ze]:null}function Bu(e,n,t){if(t&4){let o=n,i=e;for(;o=o.parent,o===null&&!(t&1);)if(o=eS(i),o===null||(i=i[ar],o.type&10))break;if(o===null)return!1;n=o,e=i}let r=K.lFrame=Ig();return r.currentTNode=n,r.lView=e,!0}function ic(e){let n=Ig(),t=e[O];K.lFrame=n,n.currentTNode=t.firstChild,n.lView=e,n.tView=t,n.contextLView=e,n.bindingIndex=t.bindingStartIndex,n.inI18n=!1}function Ig(){let e=K.lFrame,n=e===null?null:e.child;return n===null?kg(e):n}function kg(e){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:e,child:null,inI18n:!1};return e!==null&&(e.child=n),n}function Mg(){let e=K.lFrame;return K.lFrame=e.parent,e.currentTNode=null,e.lView=null,e}var Uu=Mg;function ac(){let e=Mg();e.isParent=!0,e.tView=null,e.selectedIndex=-1,e.contextLView=null,e.elementDepthCount=0,e.currentDirectiveIndex=-1,e.currentNamespace=null,e.bindingRootIndex=-1,e.bindingIndex=-1,e.currentQueryIndex=0}function Ng(e){return(K.lFrame.contextLView=hg(e,K.lFrame.contextLView))[Me]}function Ut(){return K.lFrame.selectedIndex}function cr(e){K.lFrame.selectedIndex=e}function Yi(){let e=K.lFrame;return Js(e.tView,e.selectedIndex)}function Lo(){K.lFrame.currentNamespace=Pr}function sc(){tS()}function tS(){K.lFrame.currentNamespace=null}function Hu(){return K.lFrame.currentNamespace}var Tg=!0;function cc(){return Tg}function lc(e){Tg=e}function nu(e,n=null,t=null,r){let o=zu(e,n,t,r);return o.resolveInjectorInitializers(),o}function zu(e,n=null,t=null,r,o=new Set){let i=[t||et,og(e)],a;return new Fr(i,n||To(),a||null,o)}var xe=class e{static THROW_IF_NOT_FOUND=Tr;static NULL=new ko;static create(n,t){if(Array.isArray(n))return nu({name:""},t,n,"");{let r=n.name??"";return nu({name:r},n.parent,n.providers,r)}}static \u0275prov=G({token:e,providedIn:"any",factory:()=>H(gu)});static __NG_ELEMENT_ID__=-1},ye=new C(""),tt=class{static __NG_ELEMENT_ID__=nS;static __NG_ENV_ID__=n=>n},Bs=class extends tt{_lView;constructor(n){super(),this._lView=n}get destroyed(){return zr(this._lView)}onDestroy(n){let t=this._lView;return nc(t,n),()=>gg(t,n)}};function nS(){return new Bs(z())}var Ag=!1,Rg=new C(""),lr=(()=>{class e{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new ze(!1);debugTaskTracker=f(Rg,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new Z(t=>{t.next(!1),t.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),this.debugTaskTracker?.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.debugTaskTracker?.remove(t),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=G({token:e,providedIn:"root",factory:()=>new e})}return e})(),ru=class extends q{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,cg()&&(this.destroyRef=f(tt,{optional:!0})??void 0,this.pendingTasks=f(lr,{optional:!0})??void 0)}emit(n){let t=k(null);try{super.next(n)}finally{k(t)}}subscribe(n,t,r){let o=n,i=t||(()=>null),a=r;if(n&&typeof n=="object"){let c=n;o=c.next?.bind(c),i=c.error?.bind(c),a=c.complete?.bind(c)}this.__isAsync&&(i=this.wrapInTimeout(i),o&&(o=this.wrapInTimeout(o)),a&&(a=this.wrapInTimeout(a)));let s=super.subscribe({next:o,error:i,complete:a});return n instanceof Oe&&n.add(s),s}wrapInTimeout(n){return t=>{let r=this.pendingTasks?.add();setTimeout(()=>{try{n(t)}finally{r!==void 0&&this.pendingTasks?.remove(r)}})}}},de=ru;function Us(...e){}function $u(e){let n,t;function r(){e=Us;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{e(),r()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{e(),r()})),()=>r()}function Og(e){return queueMicrotask(()=>e()),()=>{e=Us}}var Fg="isAngularZone",Y=class e{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new de(!1);onMicrotaskEmpty=new de(!1);onStable=new de(!1);onError=new de(!1);constructor(n){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:r=!1,shouldCoalesceRunChangeDetection:o=!1,scheduleInRootZone:i=Ag}=n;if(typeof Zone>"u")throw new I(908,!1);Zone.assertZonePatched();let a=this;a._nesting=0,a._outer=a._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(a._inner=a._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(a._inner=a._inner.fork(Zone.longStackTraceZoneSpec)),a.shouldCoalesceEventChangeDetection=!o&&r,a.shouldCoalesceRunChangeDetection=o,a.callbackScheduled=!1,a.scheduleInRootZone=i,iS(a)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Fg)===!0}static assertInAngularZone(){if(!e.isInAngularZone())throw new I(909,!1)}static assertNotInAngularZone(){if(e.isInAngularZone())throw new I(909,!1)}run(n,t,r){return this._inner.run(n,t,r)}runTask(n,t,r,o){let i=this._inner,a=i.scheduleEventTask("NgZoneEvent: "+o,n,rS,Us,Us);try{return i.runTask(a,t,r)}finally{i.cancelTask(a)}}runGuarded(n,t,r){return this._inner.runGuarded(n,t,r)}runOutsideAngular(n){return this._outer.run(n)}},rS={};function Gu(e){if(e._nesting==0&&!e.hasPendingMicrotasks&&!e.isStable)try{e._nesting++,e.onMicrotaskEmpty.emit(null)}finally{if(e._nesting--,!e.hasPendingMicrotasks)try{e.runOutsideAngular(()=>e.onStable.emit(null))}finally{e.isStable=!0}}}function oS(e){if(e.isCheckStableRunning||e.callbackScheduled)return;e.callbackScheduled=!0;function n(){$u(()=>{e.callbackScheduled=!1,ou(e),e.isCheckStableRunning=!0,Gu(e),e.isCheckStableRunning=!1})}e.scheduleInRootZone?Zone.root.run(()=>{n()}):e._outer.run(()=>{n()}),ou(e)}function iS(e){let n=()=>{oS(e)};e._inner=e._inner.fork({name:"angular",properties:{[Fg]:!0},onInvokeTask:(t,r,o,i,a,s)=>{if(aS(s))return t.invokeTask(o,i,a,s);try{return Gh(e),t.invokeTask(o,i,a,s)}finally{(e.shouldCoalesceEventChangeDetection&&i.type==="eventTask"||e.shouldCoalesceRunChangeDetection)&&n(),Wh(e)}},onInvoke:(t,r,o,i,a,s,c)=>{try{return Gh(e),t.invoke(o,i,a,s,c)}finally{e.shouldCoalesceRunChangeDetection&&!e.callbackScheduled&&!sS(s)&&n(),Wh(e)}},onHasTask:(t,r,o,i)=>{t.hasTask(o,i),r===o&&(i.change=="microTask"?(e._hasPendingMicrotasks=i.microTask,ou(e),Gu(e)):i.change=="macroTask"&&(e.hasPendingMacrotasks=i.macroTask))},onHandleError:(t,r,o,i)=>(t.handleError(o,i),e.runOutsideAngular(()=>e.onError.emit(i)),!1)})}function ou(e){e._hasPendingMicrotasks||(e.shouldCoalesceEventChangeDetection||e.shouldCoalesceRunChangeDetection)&&e.callbackScheduled===!0?e.hasPendingMicrotasks=!0:e.hasPendingMicrotasks=!1}function Gh(e){e._nesting++,e.isStable&&(e.isStable=!1,e.onUnstable.emit(null))}function Wh(e){e._nesting--,Gu(e)}var Pi=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new de;onMicrotaskEmpty=new de;onStable=new de;onError=new de;run(n,t,r){return n.apply(t,r)}runGuarded(n,t,r){return n.apply(t,r)}runOutsideAngular(n){return n()}runTask(n,t,r,o){return n.apply(t,r)}};function aS(e){return Pg(e,"__ignore_ng_zone__")}function sS(e){return Pg(e,"__scheduler_tick__")}function Pg(e,n){return!Array.isArray(e)||e.length!==1?!1:e[0]?.data?.[n]===!0}var en=class{_console=console;handleError(n){this._console.error("ERROR",n)}};function Lg(e){return Vg(e)?e:new iu(e)}function Vg(e){return e instanceof Error||typeof e=="object"&&e!==null&&typeof e.name=="string"&&typeof e.message=="string"}var iu=class extends Error{constructor(n){super(String(n),{cause:n}),this.name="ErrorBoundaryWrappedError"}},Ht=new C("",{factory:()=>{let e=f(Y),n=f(Pe),t;return r=>{e.runOutsideAngular(()=>{n.destroyed&&!t?setTimeout(()=>{throw r}):(t??=n.get(en),t.handleError(r))})}}}),jg={provide:No,useValue:()=>{let e=f(en,{optional:!0})},multi:!0},cS=new C("",{factory:()=>{let e=f(ye).defaultView;if(!e)return;let n=f(Ht),t=i=>{n(i.reason),i.preventDefault()},r=i=>{i.error?n(i.error):n(new Error(i.message,{cause:i})),i.preventDefault()},o=()=>{e.addEventListener("unhandledrejection",t),e.addEventListener("error",r)};typeof Zone<"u"?Zone.root.run(o):o(),f(tt).onDestroy(()=>{e.removeEventListener("error",r),e.removeEventListener("unhandledrejection",t)})}});function Wu(){return ir([rg(()=>{f(cS)})])}function E(e,n){let[t,r,o]=xd(e,n?.equal),i=t,a=i[Be];return i.set=r,i.update=o,i.asReadonly=dc.bind(i),i}function dc(){let e=this[Be];if(e.readonlyFn===void 0){let n=()=>this();n[Be]=e,e.readonlyFn=n}return e.readonlyFn}var Wr=new C("",{factory:()=>lS}),lS="ng";var uc=new C(""),qr=new C("",{providedIn:"platform",factory:()=>"unknown"}),qu=new C(""),Kr=new C("",{factory:()=>f(ye).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Qi=(()=>{class e{view;node;constructor(t,r){this.view=t,this.node=r}static __NG_ELEMENT_ID__=dS}return e})();function dS(){return new Qi(z(),rt())}var jn=class{},Xi=new C("",{factory:()=>!0});var Ku=new C(""),fc=(()=>{class e{static \u0275prov=G({token:e,providedIn:"root",factory:()=>new au})}return e})(),au=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let t=n.zone,r=this.queues.get(t);r.has(n)&&(r.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let t=n.zone;this.queues.has(t)||this.queues.set(t,new Set);let r=this.queues.get(t);r.has(n)||r.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[t,r]of this.queues)t===null?n||=this.flushQueue(r):n||=t.run(()=>this.flushQueue(r));n||(this.dirtyEffectCount=0)}}flushQueue(n){let t=!1;for(let r of n)r.dirty&&(this.dirtyEffectCount--,t=!0,r.run());return t}},Hs=class{constructor(n){this[Be]=n}destroy(){this[Be].destroy()}};function _n(e,n){let t=n?.injector??f(xe),r=n?.manualCleanup!==!0?t.get(tt):null,o,i=t.get(Qi,null,{optional:!0}),a=t.get(jn);return i!==null?(o=Ug(i.view,a,e),r instanceof Bs&&r._lView===i.view&&(r=null)):o=mS(e,t.get(fc),a),o.injector=t,r!==null&&(o.onDestroyFns=[r.onDestroy(()=>o.destroy())]),new Hs(o)}var Bg=T(v({},Cd),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let e=Fi(!1);try{Sd(this)}finally{Fi(e)}},cleanup(){if(!this.cleanupFns?.length)return;let e=k(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],k(e)}}}),uS=T(v({},Bg),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Xn(this),this.onDestroyFns!==null)for(let e of this.onDestroyFns)e();this.cleanup(),this.scheduler.remove(this)}}),fS=T(v({},Bg),{consumerMarkedDirty(){this.view[F]|=8192,$r(this.view),this.notifier.notify(13)},destroy(){if(Xn(this),this.onDestroyFns!==null)for(let e of this.onDestroyFns)e();this.cleanup(),this.view[Ln]?.delete(this)}});function Ug(e,n,t){let r=Object.create(fS);return r.view=e,r.zone=typeof Zone<"u"?Zone.current:null,r.notifier=n,r.userFn=t,r.fn=Hg(r,t),e[Ln]??=new Set,e[Ln].add(r),r.consumerMarkedDirty(r),r}function mS(e,n,t){let r=Object.create(uS);return r.userFn=e,r.fn=Hg(r,e),r.scheduler=n,r.notifier=t,r.zone=typeof Zone<"u"?Zone.current:null,r.scheduler.add(r),r.notifier.notify(12),r}function Hg(e,n){return()=>{n(t=>(e.cleanupFns??=[]).push(t))}}function xn(e){return typeof e=="function"&&e[Be]!==void 0}var Vf=Symbol("InputSignalNode#UNSET"),Cb=T(v({},vo),{transformFn:void 0,applyValueToInputSignal(e,n){Jn(e,n)}});function sa(e){return{toString:e}.toString()}var ae=(function(e){return e[e.TemplateCreateStart=0]="TemplateCreateStart",e[e.TemplateCreateEnd=1]="TemplateCreateEnd",e[e.TemplateUpdateStart=2]="TemplateUpdateStart",e[e.TemplateUpdateEnd=3]="TemplateUpdateEnd",e[e.LifecycleHookStart=4]="LifecycleHookStart",e[e.LifecycleHookEnd=5]="LifecycleHookEnd",e[e.OutputStart=6]="OutputStart",e[e.OutputEnd=7]="OutputEnd",e[e.BootstrapApplicationStart=8]="BootstrapApplicationStart",e[e.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",e[e.BootstrapComponentStart=10]="BootstrapComponentStart",e[e.BootstrapComponentEnd=11]="BootstrapComponentEnd",e[e.ChangeDetectionStart=12]="ChangeDetectionStart",e[e.ChangeDetectionEnd=13]="ChangeDetectionEnd",e[e.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",e[e.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",e[e.AfterRenderHooksStart=16]="AfterRenderHooksStart",e[e.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",e[e.ComponentStart=18]="ComponentStart",e[e.ComponentEnd=19]="ComponentEnd",e[e.DeferBlockStateStart=20]="DeferBlockStateStart",e[e.DeferBlockStateEnd=21]="DeferBlockStateEnd",e[e.DynamicComponentStart=22]="DynamicComponentStart",e[e.DynamicComponentEnd=23]="DynamicComponentEnd",e[e.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",e[e.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",e})(ae||{}),vc=class{previousValue;currentValue;firstChange;constructor(n,t,r){this.previousValue=n,this.currentValue=t,this.firstChange=r}isFirstChange(){return this.firstChange}};function Sb(e,n,t,r){n!==null?n.applyValueToInputSignal(n,r):e[t]=r}var wb=null,yt=(()=>{wb=zg;let e=()=>zg;return e.ngInherit=!0,e})();function wS(){return wb}function zg(e){return e.type.prototype.ngOnChanges&&(e.setInput=DS),ES}function ES(){let e=Eb(this),n=e?.current;if(n){let t=e.previous;if(t===or)e.previous=n;else for(let r in n)t[r]=n[r];e.current=null,this.ngOnChanges(n)}}function DS(e,n,t,r,o){let i=this.declaredInputs[r],a=Eb(e)||IS(e,{previous:or,current:null}),s=a.current||(a.current={}),c=a.previous,l=c[i];s[i]=new vc(l&&l.currentValue,t,c===or),Sb(e,n,o,t)}var cf="__ngSimpleChanges__";function Eb(e){return Object.hasOwn(e,cf)&&e[cf]||null}function IS(e,n){return e[cf]=n}var $g=[];var he=function(e,n=null,t){for(let r=0;r<$g.length;r++){let o=$g[r];o(e,n,t)}};function kS(e,n,t){let{ngOnChanges:r,ngOnInit:o,ngDoCheck:i}=n.type.prototype;if(r){let a=wS()(n);(t.preOrderHooks??=[]).push(e,a),(t.preOrderCheckHooks??=[]).push(e,a)}o&&(t.preOrderHooks??=[]).push(0-e,o),i&&((t.preOrderHooks??=[]).push(e,i),(t.preOrderCheckHooks??=[]).push(e,i))}function Db(e,n){for(let t=n.directiveStart,r=n.directiveEnd;t<r;t++){let i=e.data[t].type.prototype,{ngAfterContentInit:a,ngAfterContentChecked:s,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:d}=i;a&&(e.contentHooks??=[]).push(-t,a),s&&((e.contentHooks??=[]).push(t,s),(e.contentCheckHooks??=[]).push(t,s)),c&&(e.viewHooks??=[]).push(-t,c),l&&((e.viewHooks??=[]).push(t,l),(e.viewCheckHooks??=[]).push(t,l)),d!=null&&(e.destroyHooks??=[]).push(t,d)}}function hc(e,n,t){Ib(e,n,3,t)}function gc(e,n,t,r){(e[F]&3)===t&&Ib(e,n,t,r)}function Zu(e,n){let t=e[F];(t&3)===n&&(t&=16383,t+=1,e[F]=t)}function Ib(e,n,t,r){let o=r!==void 0?e[jr]&65535:0,i=r??-1,a=n.length-1,s=0;for(let c=o;c<a;c++)if(typeof n[c+1]=="number"){if(s=n[c],r!=null&&s>=r)break}else n[c]<0&&(e[jr]+=65536),(s<i||i==-1)&&(MS(e,t,n,c),e[jr]=(e[jr]&4294901760)+c+2),c++}function Gg(e,n){he(ae.LifecycleHookStart,e,n);let t=k(null);try{n.call(e)}finally{k(t),he(ae.LifecycleHookEnd,e,n)}}function MS(e,n,t,r){let o=t[r]<0,i=t[r+1],a=o?-t[r]:t[r],s=e[a];o?e[F]>>14<e[jr]>>16&&(e[F]&3)===n&&(e[F]+=16384,Gg(s,i)):Gg(s,i)}var Bo=-1,Yr=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,t,r,o){this.factory=n,this.name=o,this.canSeeViewProviders=t,this.injectImpl=r}};function NS(e){return(e.flags&8)!==0}function TS(e){return(e.flags&16)!==0}function AS(e,n,t){let r=0;for(;r<t.length;){let o=t[r];if(typeof o=="number"){if(o!==0)break;r++;let i=t[r++],a=t[r++],s=t[r++];e.setAttribute(n,a,s,i)}else{let i=o,a=t[++r];RS(i)?e.setProperty(n,i,a):e.setAttribute(n,i,a),r++}}return r}function kb(e){return e===3||e===4||e===6}function RS(e){return e.charCodeAt(0)===64}function Uo(e,n){if(!(n===null||n.length===0))if(e===null||e.length===0)e=n.slice();else{let t=-1;for(let r=0;r<n.length;r++){let o=n[r];typeof o=="number"?t=o:t===0||(t===-1||t===2?Wg(e,t,o,null,n[++r]):Wg(e,t,o,null,null))}}return e}function Wg(e,n,t,r,o){let i=0,a=e.length;if(n===-1)a=-1;else for(;i<e.length;){let s=e[i++];if(typeof s=="number"){if(s===n){a=-1;break}else if(s>n){a=i-1;break}}}for(;i<e.length;){let s=e[i];if(typeof s=="number")break;if(s===t){o!==null&&(e[i+1]=o);return}i++,o!==null&&i++}a!==-1&&(e.splice(a,0,n),i=a+1),e.splice(i++,0,t),o!==null&&e.splice(i++,0,o)}function Mb(e){return e!==Bo}function _c(e){return e&32767}function OS(e){return e>>16}function xc(e,n){let t=OS(e),r=n;for(;t>0;)r=r[ar],t--;return r}var lf=!0;function qg(e){let n=lf;return lf=e,n}var FS=256,Nb=FS-1,Tb=5,PS=0,Cn={};function LS(e,n,t){let r;typeof t=="string"?r=t.charCodeAt(0)||0:Object.hasOwn(t,Lr)&&(r=t[Lr]),r==null&&(r=t[Lr]=PS++);let o=r&Nb,i=1<<o;n.data[e+(o>>Tb)]|=i}function Cc(e,n){let t=Ab(e,n);if(t!==-1)return t;let r=n[O];r.firstCreatePass&&(e.injectorIndex=n.length,Yu(r.data,e),Yu(n,null),Yu(r.blueprint,null));let o=jf(e,n),i=e.injectorIndex;if(Mb(o)){let a=_c(o),s=xc(o,n),c=s[O].data;for(let l=0;l<8;l++)n[i+l]=s[a+l]|c[a+l]}return n[i+8]=o,i}function Yu(e,n){e.push(0,0,0,0,0,0,0,0,n)}function Ab(e,n){return e.injectorIndex===-1||e.parent&&e.parent.injectorIndex===e.injectorIndex||n[e.injectorIndex+8]===null?-1:e.injectorIndex}function jf(e,n){if(e.parent&&e.parent.injectorIndex!==-1)return e.parent.injectorIndex;let t=0,r=null,o=n;for(;o!==null;){if(r=Lb(o),r===null)return Bo;if(t++,o=o[ar],r.injectorIndex!==-1)return r.injectorIndex|t<<16}return Bo}function df(e,n,t){LS(e,n,t)}function VS(e,n){if(n==="class")return e.classes;if(n==="style")return e.styles;let t=e.attrs;if(t){let r=t.length,o=0;for(;o<r;){let i=t[o];if(kb(i))break;if(i===0)o=o+2;else if(typeof i=="number")for(o++;o<r&&typeof t[o]=="string";)o++;else{if(i===n)return t[o+1];o=o+2}}}return null}function Rb(e,n,t){if(t&8||e!==void 0)return e;qs(n,"NodeInjector")}function Ob(e,n,t,r){if(t&8&&r===void 0&&(r=null),(t&3)===0){let o=e[Bn],i=Et(void 0);try{return o?o.get(n,r,t&8):pu(n,r,t&8)}finally{Et(i)}}return Rb(r,n,t)}function Fb(e,n,t,r=0,o){if(e!==null){if(n[F]&2048&&!(r&2)){let a=HS(e,n,t,r,Cn);if(a!==Cn)return a}let i=Pb(e,n,t,r,Cn);if(i!==Cn)return i}return Ob(n,t,r,o)}function Pb(e,n,t,r,o){let i=BS(t);if(typeof i=="function"){if(!Bu(n,e,r))return r&1?Rb(o,t,r):Ob(n,t,r,o);try{let a;if(a=i(r),a==null&&!(r&8))qs(t);else return a}finally{Uu()}}else if(typeof i=="number"){let a=null,s=Ab(e,n),c=Bo,l=r&1?n[nt][Ze]:null;for((s===-1||r&4)&&(c=s===-1?jf(e,n):n[s+8],c===Bo||!Zg(r,!1)?s=-1:(a=n[O],s=_c(c),n=xc(c,n)));s!==-1;){let d=n[O];if(Kg(i,s,d.data)){let u=jS(s,n,t,a,r,l);if(u!==Cn)return u}c=n[s+8],c!==Bo&&Zg(r,n[O].data[s+8]===l)&&Kg(i,s,n)?(a=d,s=_c(c),n=xc(c,n)):s=-1}}return o}function jS(e,n,t,r,o,i){let a=n[O],s=a.data[e+8],c=r==null?Hn(s)&&lf:r!=a&&(s.type&3)!==0,l=o&1&&i===s,d=bc(s,a,t,c,l);return d!==null?ta(n,a,d,s,o):Cn}function bc(e,n,t,r,o){let i=e.providerIndexes,a=n.data,s=i&1048575,c=e.directiveStart,l=e.directiveEnd,d=i>>20,u=r?s:s+d,g=o?s+d:l;for(let h=u;h<g;h++){let _=a[h];if(h<c&&t===_||h>=c&&_.type===t)return h}if(o){let h=a[c];if(h&&vn(h)&&h.type===t)return c}return null}function ta(e,n,t,r,o){let i=e[t],a=n.data;if(i instanceof Yr){let s=i;if(s.resolving)throw mu("");let c=qg(s.canSeeViewProviders);s.resolving=!0;let l=a[t].type||a[t],d,u=s.injectImpl?Et(s.injectImpl):null,g=Bu(e,r,0);try{i=e[t]=s.factory(void 0,o,a,e,r),n.firstCreatePass&&t>=r.directiveStart&&kS(t,a[t],n)}finally{u!==null&&Et(u),qg(c),s.resolving=!1,Uu()}}return i}function BS(e){if(typeof e=="string")return e.charCodeAt(0)||0;let n=Object.hasOwn(e,Lr)?e[Lr]:void 0;return typeof n=="number"?n>=0?n&Nb:US:n}function Kg(e,n,t){let r=1<<e;return!!(t[n+(e>>Tb)]&r)}function Zg(e,n){return!(e&2)&&!(e&1&&n)}var zn=class{_tNode;_lView;constructor(n,t){this._tNode=n,this._lView=t}get(n,t,r){return Fb(this._tNode,this._lView,n,Ar(r),t)}};function US(){return new zn(rt(),z())}function qo(e){return sa(()=>{let n=e.prototype.constructor,t=n[Oi]||uf(n),r=Object.prototype,o=Object.getPrototypeOf(e.prototype).constructor;for(;o&&o!==r;){let i=o[Oi]||uf(o);if(i&&i!==t)return i;o=Object.getPrototypeOf(o)}return i=>new i})}function uf(e){return cu(e)?()=>{let n=uf(qe(e));return n&&n()}:Rr(e)}function HS(e,n,t,r,o){let i=e,a=n;for(;i!==null&&a!==null&&a[F]&2048&&!Fo(a);){let s=Pb(i,a,t,r|2,Cn);if(s!==Cn)return s;r&=-5;let c=i.parent;if(!c){let l=a[Cu];if(l){let d=l.get(t,Cn,r);if(d!==Cn)return d}c=Lb(a),a=a[ar]}i=c}return o}function Lb(e){let n=e[O],t=n.type;return t===2?n.declTNode:t===1?e[Ze]:null}function ca(e){return VS(rt(),e)}function B(e){return{token:e.token,providedIn:e.autoProvided===!1?null:"root",factory:e.factory,value:void 0}}function zS(){return Ko(rt(),z())}function Ko(e,n){return new ue(Vt(e,n))}var ue=(()=>{class e{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=zS}return e})();function Vb(e){return e instanceof ue?e.nativeElement:e}function $S(){return this._results[Symbol.iterator]()}var Sc=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new q}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,t){return this._results.reduce(n,t)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,t){this.dirty=!1;let r=eg(n);(this._changesDetected=!Jh(this._results,r,t))&&(this._results=r,this.length=r.length,this.last=r[this.length-1],this.first=r[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=$S};function jb(e){return(e.flags&128)===128}var Bf=(function(e){return e[e.OnPush=0]="OnPush",e[e.Eager=1]="Eager",e[e.Default=1]="Default",e})(Bf||{}),Bb=new Map,GS=0;function WS(){return GS++}function qS(e){Bb.set(e[$i],e)}function ff(e){Bb.delete(e[$i])}var Yg="__ngContext__";function Ho(e,n){Un(n)?(e[Yg]=n[$i],qS(n)):e[Yg]=n}function Ub(e){return zb(e[Ro])}function Hb(e){return zb(e[Pt])}function zb(e){for(;e!==null&&!Lt(e);)e=e[Pt];return e}var mf;function Uf(e){mf=e}function $b(){if(mf!==void 0)return mf;if(typeof document<"u")return document;throw new I(210,!1)}var Gb="r";var Wb="di";var qb=!1,Kb=new C("",{factory:()=>qb});var Qg=new WeakMap;function KS(e,n){if(e==null||typeof e!="object")return;let t=Qg.get(e);t||(t=new WeakSet,Qg.set(e,t)),t.add(n)}var ZS=(e,n,t,r)=>{};function YS(e,n,t,r){ZS(e,n,t,r)}function Pc(e){return(e.flags&32)===32}var QS=()=>null;function Zb(e,n,t=!1){return QS(e,n,t)}function Yb(e,n){let t=e.contentQueries;if(t!==null){let r=k(null);try{for(let o=0;o<t.length;o+=2){let i=t[o],a=t[o+1];if(a!==-1){let s=e.data[a];Zi(i),s.contentQueries(2,n[a],a)}}}finally{k(r)}}}function pf(e,n,t){Zi(0);let r=k(null);try{n(e,t)}finally{k(r)}}function Qb(e,n,t){if(wu(n)){let r=k(null);try{let o=n.directiveStart,i=n.directiveEnd;for(let a=o;a<i;a++){let s=e.data[a];if(s.contentQueries){let c=t[a];s.contentQueries(1,c,a)}}}finally{k(r)}}}var an=(function(e){return e[e.Emulated=0]="Emulated",e[e.None=2]="None",e[e.ShadowDom=3]="ShadowDom",e[e.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",e})(an||{}),wc=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity}`}};function Qr(e){return e instanceof wc?e.changingThisBreaksApplicationSecurity:e}function Hf(e,n){let t=Xb(e);if(t!=null&&t!==n){if(t==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${t} (see ${su})`)}return t===n}function Xb(e){return e instanceof wc&&e.getTypeName()||null}var XS={"http://www.w3.org/2000/svg":Pr,"http://www.w3.org/1998/Math/MathML":Xs};var mc;function JS(){if(mc===void 0&&(mc=null,Ui.trustedTypes))try{mc=Ui.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:e=>e,createScript:e=>e,createScriptURL:e=>e})}catch{}return mc}function Qu(e){return JS()?.createScriptURL(e)||e}var ew=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Jb(e){return e=String(e),e.match(ew)?e:"unsafe:"+e}function tw(e,n){return e.createText(n)}function nw(e,n,t){e.setValue(n,t)}function ey(e,n,t){return e.createElement(n,t)}function Zr(e,n,t,r,o){e.insertBefore(n,t,r,o)}function ty(e,n,t){e.appendChild(n,t)}function Xg(e,n,t,r,o){r!==null?Zr(e,n,t,r,o):ty(e,n,t)}function ny(e,n,t,r){e.removeChild(null,n,t,r)}function rw(e,n,t,r,o,i,a){if(i==null)a?.(i,r||"",o),e.removeAttribute(n,o,t);else{let s=a==null?bn(i):a(i,r||"",o);e.setAttribute(n,o,s,t)}}function ow(e,n,t){e.setAttribute(n,"style",t)}function iw(e,n,t){t===""?e.removeAttribute(n,"class"):e.setAttribute(n,"class",t)}function ry(e,n,t){let{mergedAttrs:r,classes:o,styles:i}=t;r!==null&&AS(e,n,r),o!==null&&iw(e,n,o),i!==null&&ow(e,n,i)}function aw(e,n=!0){if(e[0]!=":")return[null,e];let t=e.indexOf(":",1);if(t===-1){if(n)throw new Error(`Unsupported format "${e}" expecting ":namespace:name"`);return[null,e]}return[e.slice(1,t),e.slice(t+1)]}function oy(e){let n=ay();return n?n.sanitize(We.URL,e)||"":Hf(e,"URL")?Qr(e):Jb(bn(e))}function iy(e){let n=ay();if(n)return Qu(n.sanitize(We.RESOURCE_URL,e)||"");if(e==null)return Qu("");if(Hf(e,"ResourceURL"))return Qu(Qr(e));throw new I(-904,!1)}function sw(e,n){switch(cw(e,n)){case We.RESOURCE_URL:return iy;case We.URL:return oy;default:return null}}function zf(e,n,t){return sw(n,t)?.(e)??e}function ay(){let e=z();return e&&e[nn].sanitizer}function cw(e,n){let[t,r]=lw(e);return ug(r,n,t)}function lw(e){e=e.toLowerCase();let n=aw(e,!1);if(n[0])return n;let r=Ut()===-1?null:Yi(),o=r?.namespace;if(e==="#host"&&r?.type===2){let i=Vt(r,z());if(i.tagName&&(e=i.tagName.toLowerCase()),o==null){let a=i.namespaceURI;o=a&&XS[a]}}return[o,e]}function dw(e){return e instanceof Function?e():e}function uw(e,n,t){let r=e.length;for(;;){let o=e.indexOf(n,t);if(o===-1)return o;if(o===0||e.charCodeAt(o-1)<=32){let i=n.length;if(o+i===r||e.charCodeAt(o+i)<=32)return o}t=o+1}}var sy="ng-template";function fw(e,n,t,r){let o=0;if(r){for(;o<n.length&&typeof n[o]=="string";o+=2)if(n[o]==="class"&&uw(n[o+1].toLowerCase(),t,0)!==-1)return!0}else if($f(e))return!1;if(o=n.indexOf(1,o),o>-1){let i;for(;++o<n.length&&typeof(i=n[o])=="string";)if(i.toLowerCase()===t)return!0}return!1}function $f(e){return e.type===4&&e.value!==sy}function mw(e,n,t){let r=e.type===4&&!t?sy:e.value;return n===r}function pw(e,n,t){let r=4,o=e.attrs,i=o!==null?bw(o):0,a=!1;for(let s=0;s<n.length;s++){let c=n[s];if(typeof c=="number"){if(!a&&!rn(r)&&!rn(c))return!1;if(a&&rn(c))continue;a=!1,r=c|r&1;continue}if(!a)if(r&4){if(r=2|r&1,c!==""&&!mw(e,c,t)||c===""&&n.length===1){if(rn(r))return!1;a=!0}}else if(r&8){if(o===null||!fw(e,o,c,t)){if(rn(r))return!1;a=!0}}else{let l=n[++s],d=hw(c,o,$f(e),t);if(d===-1){if(rn(r))return!1;a=!0;continue}if(l!==""){let u;if(d>i?u="":u=o[d+1].toLowerCase(),r&2&&l!==u){if(rn(r))return!1;a=!0}}}}return rn(r)||a}function rn(e){return(e&1)===0}function hw(e,n,t,r){if(n===null)return-1;let o=0;if(r||!t){let i=!1;for(;o<n.length;){let a=n[o];if(a===e)return o;if(a===3||a===6)i=!0;else if(a===1||a===2){let s=n[++o];for(;typeof s=="string";)s=n[++o];continue}else{if(a===4)break;if(a===0){o+=4;continue}}o+=i?1:2}return-1}else return yw(n,e)}function cy(e,n,t=!1){for(let r=0;r<n.length;r++)if(pw(e,n[r],t))return!0;return!1}function gw(e){let n=e.attrs;if(n!=null){let t=n.indexOf(5);if((t&1)===0)return n[t+1]}return null}function bw(e){for(let n=0;n<e.length;n++){let t=e[n];if(kb(t))return n}return e.length}function yw(e,n){let t=e.indexOf(4);if(t>-1)for(t++;t<e.length;){let r=e[t];if(typeof r=="number")return-1;if(r===n)return t;t++}return-1}function vw(e,n){e:for(let t=0;t<n.length;t++){let r=n[t];if(e.length===r.length){for(let o=0;o<e.length;o++)if(e[o]!==r[o])continue e;return!0}}return!1}function Jg(e,n){return e?":not("+n.trim()+")":n}function _w(e){let n=e[0],t=1,r=2,o="",i=!1;for(;t<e.length;){let a=e[t];if(typeof a=="string")if(r&2){let s=e[++t];o+="["+a+(s.length>0?'="'+s+'"':"")+"]"}else r&8?o+="."+a:r&4&&(o+=" "+a);else o!==""&&!rn(a)&&(n+=Jg(i,o),o=""),r=a,i=i||!rn(r);t++}return o!==""&&(n+=Jg(i,o)),n}function xw(e){return e.map(_w).join(",")}function Cw(e){let n=[],t=[],r=1,o=2;for(;r<e.length;){let i=e[r];if(typeof i=="string")o===2?i!==""&&n.push(i,e[++r]):o===8&&t.push(i);else{if(!rn(o))break;o=i}r++}return t.length&&n.push(1,...t),n}var ut={},Sn=(function(e){return e[e.Important=1]="Important",e[e.DashCase=2]="DashCase",e})(Sn||{}),Sw;function Gf(e,n){return Sw(e,n)}var Wf=new Set;var cj=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var hf=new WeakMap;function ly(e){return e?e[ar]??e:null}var Vo=new WeakSet;function ww(e,n,t){let r=hf.get(e);if(!r||r.length===0)return;let o=n.parentNode,i=n.previousSibling,a=ly(t);for(let s=r.length-1;s>=0;s--){let{el:c,declarationView:l}=r[s],d=c.parentNode;c===n?(r.splice(s,1),Vo.add(c),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):i&&c===i?(r.splice(s,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c)):d&&o&&d!==o&&(a===null||l===null||a===l)&&(r.splice(s,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c))}}function Ew(e,n,t){let r=ly(t),o=hf.get(e);o?o.some(i=>i.el===n)||o.push({el:n,declarationView:r}):hf.set(e,[{el:n,declarationView:r}])}var Lc=(function(e){return e[e.CHANGE_DETECTION=0]="CHANGE_DETECTION",e[e.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",e})(Lc||{}),$n=new C(""),eb=new Set;function eo(e){eb.has(e)||(eb.add(e),performance?.mark?.("mark_feature_usage",{detail:{feature:e}}))}var qf=(()=>{class e{impl=null;execute(){this.impl?.execute()}static \u0275prov=G({token:e,providedIn:"root",factory:()=>new e})}return e})(),Kf=[0,1,2,3],dy=(()=>{class e{ngZone=f(Y);scheduler=f(jn);errorHandler=f(en,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){f($n,{optional:!0})}execute(){let t=this.sequences.size>0;t&&he(ae.AfterRenderHooksStart),this.executing=!0;for(let r of Kf)for(let o of this.sequences)if(!(o.erroredOrDestroyed||!o.hooks[r]))try{o.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let i=o.hooks[r];return i(o.pipelinedValue)},o.snapshot))}catch(i){o.erroredOrDestroyed=!0,this.errorHandler?.handleError(i)}this.executing=!1;for(let r of this.sequences)r.afterRun(),r.once&&(this.sequences.delete(r),r.destroy());for(let r of this.deferredRegistrations)this.sequences.add(r);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),t&&he(ae.AfterRenderHooksEnd)}register(t){let{view:r}=t;r!==void 0?((r[Br]??=[]).push(t),$r(r),r[F]|=8192):this.addSequence(t)}addSequence(t){this.executing?this.deferredRegistrations.add(t):this.sequences.add(t),this.scheduler.notify(7)}unregister(t){this.executing&&this.sequences.has(t)?(t.erroredOrDestroyed=!0,t.pipelinedValue=void 0,t.once=!0):(this.sequences.delete(t),this.deferredRegistrations.delete(t))}maybeTrace(t,r){return r?r.run(Lc.AFTER_NEXT_RENDER,t):t()}static \u0275prov=G({token:e,providedIn:"root",factory:()=>new e})}return e})(),Ec=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,t,r,o,i,a=null){this.impl=n,this.hooks=t,this.view=r,this.once=o,this.snapshot=a,this.unregisterOnDestroy=i?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[Br];n&&(this.view[Br]=n.filter(t=>t!==this))}};var gf=null,uy=null;function tb(e,n,t,r){gf?.maybeQueueEnterAnimation(e,n,t,r)}function nb(e,n,t,r){gf===null?r(!1):gf.runLeaveAnimationsWithCallback(e,n,t,r)}function Dw(e){uy?.initViewDetachAnimations(e)}function Iw(e){uy?.clearViewDetachAnimations(e)}function jo(e,n,t,r,o,i,a,s){if(o!=null){let c,l=!1;Lt(o)?c=o:Un(o)&&(l=!0,o=o[tn]);let d=Ye(o);e===0&&r!==null?(tb(s,r,i,t),a==null?ty(n,r,d):Zr(n,r,d,a||null,!0)):e===1&&r!==null?(Zr(n,r,d,a||null,!0),ww(i,d,s),Vo.has(d)||tb(s,r,i,t)):e===2?(s?.[lg]?.leave?.has(i.index)&&Ew(i,d,s),Vo.delete(d),nb(s,i,t,u=>{if(Vo.has(d)){Vo.delete(d);return}ny(n,d,l,u)})):e===3&&(Vo.delete(d),nb(s,i,t,()=>{n.destroyNode(d)})),c!=null&&Vw(n,e,t,c,i,r,a)}}function kw(e,n){fy(e,n),n[tn]=null,n[Ze]=null}function Mw(e,n,t,r,o,i){r[tn]=o,r[Ze]=n,jc(e,r,t,1,o,i)}function fy(e,n){n[nn].changeDetectionScheduler?.notify(9),jc(e,n,n[De],2,null,null)}function Nw(e){let n=e[Ro];if(!n)return Xu(e[O],e);for(;n;){let t=null;if(Un(n))t=n[Ro];else{let r=n[Ne];r&&(t=r)}if(!t){for(;n&&!n[Pt]&&n!==e;)Un(n)&&Xu(n[O],n),n=n[Le];n===null&&(n=e),Un(n)&&Xu(n[O],n),t=n&&n[Pt]}n=t}}function Zf(e,n){let t=e[Hr],r=t.indexOf(n);t.splice(r,1)}function Vc(e,n){if(zr(n))return;let t=n[De];t.destroyNode&&jc(e,n,t,3,null,null),Nw(n)}function Xu(e,n){if(zr(n))return;let t=k(null);try{n[F]&=-129,n[F]|=256,n[Dt]&&Xn(n[Dt]),Aw(e,n),Tw(e,n),n[O].type===1&&n[De].destroy();let r=n[sr];if(r!==null&&Lt(n[Le])){r!==n[Le]&&Zf(r,n);let o=n[yn];o!==null&&o.detachView(e)}ff(n)}finally{k(t)}}function Tw(e,n){let t=e.cleanup,r=n[Ao];if(t!==null)for(let a=0;a<t.length-1;a+=2)if(typeof t[a]=="string"){let s=t[a+3];s>=0?r[s]():r[-s].unsubscribe(),a+=2}else{let s=r[t[a+1]];t[a].call(s)}r!==null&&(n[Ao]=null);let o=n[Pn];if(o!==null){n[Pn]=null;for(let a=0;a<o.length;a++){let s=o[a];s()}}let i=n[Ln];if(i!==null){n[Ln]=null;for(let a of i)a.destroy()}}function Aw(e,n){let t;if(e!=null&&(t=e.destroyHooks)!=null)for(let r=0;r<t.length;r+=2){let o=n[t[r]];if(!(o instanceof Yr)){let i=t[r+1];if(Array.isArray(i))for(let a=0;a<i.length;a+=2){let s=o[i[a]],c=i[a+1];he(ae.LifecycleHookStart,s,c);try{c.call(s)}finally{he(ae.LifecycleHookEnd,s,c)}}else{he(ae.LifecycleHookStart,o,i);try{i.call(o)}finally{he(ae.LifecycleHookEnd,o,i)}}}}}function my(e,n,t){if(n===null)throw new I(510,!1);return Rw(e,n.parent,t)}function Rw(e,n,t){let r=n;for(;r!==null&&r.type&168;)n=r,r=n.parent;if(r===null)return t[tn];if(Hn(r)){let{encapsulation:o}=e.data[r.directiveStart+r.componentOffset];if(o===an.None||o===an.Emulated)return null}return Vt(r,t)}function py(e,n,t){return Fw(e,n,t)}function Ow(e,n,t){return e.type&40?Vt(e,t):null}var Fw=Ow,rb;function Yf(e,n,t,r){let o=my(e,r,n),i=n[De],a=r.parent||n[Ze],s=py(a,r,n);if(o!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)Xg(i,o,t[c],s,!1);else Xg(i,o,t,s,!1);rb!==void 0&&rb(i,r,n,t,o)}function Ji(e,n){if(n!==null){let t=n.type;if(t&3)return Vt(n,e);if(t&4)return bf(-1,e[n.index]);if(t&8){let r=n.child;if(r!==null)return Ji(e,r);{let o=e[n.index];return Lt(o)?bf(-1,o):Ye(o)}}else{if(t&128)return Ji(e,n.next);if(t&32)return Gf(n,e)()||Ye(e[n.index]);{let r=hy(e,n);if(r!==null){if(Array.isArray(r))return r[0];let o=Vn(e[nt]);return Ji(o,r)}else return Ji(e,n.next)}}}return null}function hy(e,n){if(n!==null){let r=e[nt][Ze],o=n.projection;return r.projection[o]}return null}function bf(e,n){let t=Ne+e+1;if(t<n.length){let r=n[t],o=r[O].firstChild;if(o!==null)return Ji(r,o)}return n[Ur]}function Qf(e,n,t,r,o,i,a){for(;t!=null;){let s=r[Bn];if(t.type===128){t=t.next;continue}let c=r[t.index],l=t.type;if(a&&n===0&&(c&&Ho(Ye(c),r),t.flags|=2),!Pc(t))if(l&8)Qf(e,n,t.child,r,o,i,!1),jo(n,e,s,o,c,t,i,r);else if(l&32){let d=Gf(t,r),u;for(;u=d();)jo(n,e,s,o,u,t,i,r);jo(n,e,s,o,c,t,i,r)}else l&16?gy(e,n,r,t,o,i):jo(n,e,s,o,c,t,i,r);t=a?t.projectionNext:t.next}}function jc(e,n,t,r,o,i){e.type===3?Pw(t,r,n,o,i):Qf(t,r,e.firstChild,n,o,i,!1)}function Pw(e,n,t,r,o){let a=t[O].firstChild,s=a.next,c=Ye(t[a.index]),l=Ye(t[s.index]),d=s.index+1,u=t[d];if(n===1||n===0)r!==null&&(u&&u.hasChildNodes()?Zr(e,r,u,o,!0):(Zr(e,r,c,o,!0),Zr(e,r,l,o,!0)));else if(n===2){if(u||(u=document.createDocumentFragment(),t[d]=u),c&&c.parentNode===u)return;let g=c;for(;g!==null;){let h=g.nextSibling;if(u.appendChild(g),g===l)break;g=h}}}function Lw(e,n,t){let r=n[De],o=my(e,t,n),i=t.parent||n[Ze],a=py(i,t,n);gy(r,0,n,t,o,a)}function gy(e,n,t,r,o,i){let a=t[nt],c=a[Ze].projection[r.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let d=c[l];jo(n,e,t[Bn],o,d,r,i,t)}else{let l=c,d=a[Le];jb(r)&&(l.flags|=128),Qf(e,n,l,d,o,i,!0)}}function Vw(e,n,t,r,o,i,a){let s=r[Ur],c=Ye(r);if(s!==c&&jo(n,e,t,i,s,o,a),(r[F]&4)===0)for(let l=Ne;l<r.length;l++){let d=r[l];jc(d[O],d,e,n,i,s)}}function jw(e,n,t,r,o){if(n)o?e.addClass(t,r):e.removeClass(t,r);else{let i=r.indexOf("-")===-1?void 0:Sn.DashCase;o==null?e.removeStyle(t,r,i):(typeof o=="string"&&o.endsWith("!important")&&(o=o.slice(0,-10),i|=Sn.Important),e.setStyle(t,r,o,i))}}function Xf(e,n,t,r,o,i,a,s,c,l,d){let u=Ve+r,g=u+o,h=Bw(u,g),_=typeof l=="function"?l():l;return h[O]={type:e,blueprint:h,template:t,queries:null,viewQuery:s,declTNode:n,data:h.slice().fill(null,u),bindingStartIndex:u,expandoStartIndex:g,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof i=="function"?i():i,pipeRegistry:typeof a=="function"?a():a,firstChild:null,schemas:c,consts:_,incompleteFirstPass:!1,ssrId:d}}function Bw(e,n){let t=[];for(let r=0;r<n;r++)t.push(r<e?null:ut);return t}function Uw(e){let n=e.tView;return n===null||n.incompleteFirstPass?e.tView=Xf(1,null,e.template,e.decls,e.vars,e.directiveDefs,e.pipeDefs,e.viewQuery,e.schemas,e.consts,e.id):n}function Jf(e,n,t,r,o,i,a,s,c,l,d){let u=n.blueprint.slice();return u[tn]=o,u[F]=r|4|128|8|64|1024,(l!==null||e&&e[F]&2048)&&(u[F]|=2048),Du(u),u[Le]=u[ar]=e,u[Me]=t,u[nn]=a||e&&e[nn],u[De]=s||e&&e[De],u[Bn]=c||e&&e[Bn]||null,u[Ze]=i,u[$i]=WS(),u[Vr]=d,u[Cu]=l,u[nt]=n.type==2?e[nt]:u,u}function Hw(e,n,t){let r=Vt(n,e),o=Uw(t),i=e[nn].rendererFactory,a=em(e,Jf(e,o,null,by(t),r,n,null,i.createRenderer(r,t),null,null,null));return e[n.index]=a}function by(e){let n=16;return e.signals?n=4096:e.onPush&&(n=64),n}function yy(e,n,t,r){if(t===0)return-1;let o=n.length;for(let i=0;i<t;i++)n.push(r),e.blueprint.push(r),e.data.push(null);return o}function em(e,n){return e[Ro]?e[xu][Pt]=n:e[Ro]=n,e[xu]=n,n}function y(e=1){vy(Ue(),z(),Ut()+e,!1)}function vy(e,n,t,r){if(!r)if((n[F]&3)===3){let i=e.preOrderCheckHooks;i!==null&&hc(n,i,t)}else{let i=e.preOrderHooks;i!==null&&gc(n,i,0,t)}cr(t)}var Bc=(function(e){return e[e.None=0]="None",e[e.SignalBased=1]="SignalBased",e[e.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",e})(Bc||{});function yf(e,n,t,r){let o=k(null);try{let[i,a,s]=e.inputs[t],c=null;(a&Bc.SignalBased)!==0&&(c=n[i][Be]),c!==null&&c.transformFn!==void 0?r=c.transformFn(r):s!==null&&(r=s.call(n,r)),e.setInput!==null?e.setInput(n,c,r,t,i):Sb(n,c,i,r)}finally{k(o)}}function _y(e,n,t,r,o){let i=Ut(),a=r&2;try{cr(-1),a&&n.length>Ve&&vy(e,n,Ve,!1);let s=a?ae.TemplateUpdateStart:ae.TemplateCreateStart;he(s,o,t),t(r,o)}finally{cr(i);let s=a?ae.TemplateUpdateEnd:ae.TemplateCreateEnd;he(s,o,t)}}function tm(e,n,t){Zw(e,n,t),(t.flags&64)===64&&Yw(e,n,t)}function Uc(e,n,t=Vt){let r=n.localNames;if(r!==null){let o=n.index+1;for(let i=0;i<r.length;i+=2){let a=r[i+1],s=a===-1?t(n,e):e[a];e[o++]=s}}}function zw(e,n,t,r){let i=r.get(Kb,qb)||t===an.ShadowDom||t===an.ExperimentalIsolatedShadowDom,a=e.selectRootElement(n,i);return $w(a),a}function $w(e){Gw(e)}var Gw=()=>null;function Ww(e){return e==="class"?"className":e==="for"?"htmlFor":e==="formaction"?"formAction":e==="innerHtml"?"innerHTML":e==="readonly"?"readOnly":e==="tabindex"?"tabIndex":e}function qw(e,n,t,r,o,i){let a=n[O];if(nm(e,a,n,t,r)){Hn(e)&&Kw(n,e.index);return}e.type&3&&(t=Ww(t)),xy(e,n,t,r,o,i)}function xy(e,n,t,r,o,i){if(e.type&3){let a=Vt(e,n);r=i!=null?i(r,e.value||"",t):r,r=Cy(t,a,r),o.setProperty(a,t,r)}else e.type&12}function Cy(e,n,t,r=!1){return e==="src"&&(n.tagName==="IMG"||n.tagName==="VIDEO")?t==null?r?t:"":Qr(t):t}function Kw(e,n){let t=jt(n,e);t[F]&16||(t[F]|=64)}function Zw(e,n,t){let r=t.directiveStart,o=t.directiveEnd;Hn(t)&&Hw(n,t,e.data[r+t.componentOffset]),e.firstCreatePass||Cc(t,n);let i=t.initialInputs;for(let a=r;a<o;a++){let s=e.data[a],c=ta(n,e,a,t);if(Ho(c,n),i!==null&&Jw(n,a-r,c,s,t,i),vn(s)){let l=jt(t.index,n);l[Me]=ta(n,e,a,t)}}}function Yw(e,n,t){let r=t.directiveStart,o=t.directiveEnd,i=t.index,a=Eg();try{cr(i);for(let s=r;s<o;s++){let c=e.data[s],l=n[s];rc(s),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&Qw(c,l)}}finally{cr(-1),rc(a)}}function Qw(e,n){e.hostBindings!==null&&e.hostBindings(1,n)}function Sy(e,n){let t=e.directiveRegistry,r=null;if(t)for(let o=0;o<t.length;o++){let i=t[o];cy(n,i.selectors,!1)&&(r??=[],vn(i)?r.unshift(i):r.push(i))}return r}function Xw(e,n,t,r,o,i){let a=Vt(e,n);r=Cy(t,a,r,!0),rw(n[De],a,i,e.value,t,r,o)}function Jw(e,n,t,r,o,i){let a=i[n];if(a!==null)for(let s=0;s<a.length;s+=2){let c=a[s],l=a[s+1];yf(r,t,c,l)}}function wy(e,n,t,r,o){let i=Ve+t,a=n[O],s=o(a,n,e,r,t);n[i]=s,Po(e,!0);let c=e.type===2;return c?(ry(n[De],s,e),(yg()===0||Wi(e))&&Ho(s,n),vg()):Ho(s,n),cc()&&(!c||!Pc(e))&&Yf(a,n,s,e),e}function Ey(e){let n=e;return Pu()?Lu():(n=n.parent,Po(n,!1)),n}function eE(e,n){let t=e[Bn];if(!t)return;let r;try{r=t.get(Ht,null)}catch{r=null}r?.(n)}function nm(e,n,t,r,o){let i=e.inputs?.[r],a=e.hostDirectiveInputs?.[r],s=!1;if(a)for(let c=0;c<a.length;c+=2){let l=a[c],d=a[c+1],u=n.data[l];yf(u,t[l],d,o),s=!0}if(i)for(let c of i){let l=t[c],d=n.data[c];yf(d,l,r,o),s=!0}return s}function tE(e,n){let t=jt(n,e),r=t[O];nE(r,t);let o=t[tn];o!==null&&t[Vr]===null&&(t[Vr]=Zb(o,t[Bn])),he(ae.ComponentStart);try{rm(r,t,t[Me])}finally{he(ae.ComponentEnd,t[Me])}}function nE(e,n){for(let t=n.length;t<e.blueprint.length;t++)n.push(e.blueprint[t])}function rm(e,n,t){ic(n);try{let r=e.viewQuery;r!==null&&pf(1,r,t);let o=e.template;o!==null&&_y(e,n,o,1,t),e.firstCreatePass&&(e.firstCreatePass=!1),n[yn]?.finishViewCreation(e),e.staticContentQueries&&Yb(e,n),e.staticViewQueries&&pf(2,e.viewQuery,t);let i=e.components;i!==null&&rE(n,i)}catch(r){throw e.firstCreatePass&&(e.incompleteFirstPass=!0,e.firstCreatePass=!1),r}finally{n[F]&=-5,ac()}}function rE(e,n){for(let t=0;t<n.length;t++)tE(e,n[t])}function la(e,n,t,r){let o=k(null);try{let i=n.tView,s=e[F]&4096?4096:16,c=Jf(e,i,t,s,null,n,null,null,r?.injector??null,r?.embeddedViewInjector??null,r?.dehydratedView??null),l=e[n.index];c[sr]=l;let d=e[yn];return d!==null&&(c[yn]=d.createEmbeddedView(i)),rm(i,c,t),c}finally{k(o)}}function zo(e,n){return!n||n.firstChild===null||jb(e)}function na(e,n,t,r,o=!1){if(e.type===3){let i=e.firstChild,a=i.next,s=Ye(n[i.index]),c=Ye(n[a.index]),l=s;for(;l!==null&&(r.push(l),l!==c);)l=l.nextSibling;return r}for(;t!==null;){if(t.type===128){t=o?t.projectionNext:t.next;continue}let i=n[t.index];if(i!==null)if(Lt(i)){let s=i[Ur];s!==i[tn]&&r.push(Ye(i)),i[F]&4||Dy(i,r),r.push(s)}else r.push(Ye(i));let a=t.type;if(a&8)na(e,n,t.child,r);else if(a&32){let s=Gf(t,n),c;for(;c=s();)r.push(c)}else if(a&16){let s=hy(n,t);if(Array.isArray(s))r.push(...s);else{let c=Vn(n[nt]);na(c[O],c,s,r,!0)}}t=o?t.projectionNext:t.next}return r}function Dy(e,n){for(let t=Ne;t<e.length;t++){let r=e[t],o=r[O].firstChild;o!==null&&na(r[O],r,o,n)}}function Iy(e){if(e[Br]!==null){for(let n of e[Br])n.impl.addSequence(n);e[Br].length=0}}var ky=[];function oE(e){return e[Dt]??iE(e)}function iE(e){let n=ky.pop()??Object.create(sE);return n.lView=e,n}function aE(e){e.lView[Dt]!==e&&(e.lView=null,ky.push(e))}var sE=T(v({},Zn),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:e=>{$r(e.lView)},consumerOnSignalRead(){this.lView[Dt]=this}});function cE(e){let n=e[Dt]??Object.create(lE);return n.lView=e,n}var lE=T(v({},Zn),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:e=>{let n=Vn(e.lView);for(;n&&!My(n[O]);)n=Vn(n);n&&Iu(n)},consumerOnSignalRead(){this.lView[Dt]=this}});function My(e){return e.type!==2}function Ny(e){if(e[Ln]===null)return;let n=!0;for(;n;){let t=!1;for(let r of e[Ln])if(r.dirty&&(t=!0,r.zone===null||Zone.current===r.zone?r.run():r.zone.run(()=>r.run()),e[Ln]===null))return;n=t&&!!(e[F]&8192)}}var dE=100;function Ty(e,n=0){let r=e[nn].rendererFactory,o=!1;o||r.begin?.();try{uE(e,n)}finally{o||r.end?.()}}function uE(e,n){let t=Vu();try{Fi(!0),vf(e,n);let r=0;for(;qi(e);){if(r===dE)throw new I(103,!1);r++,vf(e,1)}}finally{Fi(t)}}function fE(e,n,t,r){if(zr(n))return;let o=n[F],i=!1,a=!1;ic(n);let s=!0,c=null,l=null;i||(My(e)?(l=oE(n),c=Rn(l)):ss()===null?(s=!1,l=cE(n),c=Rn(l)):n[Dt]&&(Xn(n[Dt]),n[Dt]=null));try{Du(n),Cg(e.bindingStartIndex),t!==null&&_y(e,n,t,2,r);let d=(o&3)===3;if(!i)if(d){let h=e.preOrderCheckHooks;h!==null&&hc(n,h,null)}else{let h=e.preOrderHooks;h!==null&&gc(n,h,0,null),Zu(n,0)}if(a||mE(n),Ny(n),Ry(n,0),e.contentQueries!==null&&Yb(e,n),!i)if(d){let h=e.contentCheckHooks;h!==null&&hc(n,h)}else{let h=e.contentHooks;h!==null&&gc(n,h,1),Zu(n,1)}hE(e,n);let u=e.components;u!==null&&Fy(n,u,0);let g=e.viewQuery;if(g!==null&&pf(2,g,r),!i)if(d){let h=e.viewCheckHooks;h!==null&&hc(n,h)}else{let h=e.viewHooks;h!==null&&gc(n,h,2),Zu(n,2)}if(e.firstUpdatePass===!0&&(e.firstUpdatePass=!1),n[Qs]){for(let h of n[Qs])h();n[Qs]=null}i||(Iy(n),n[F]&=-73)}catch(d){Ay(n,d,i)}finally{l!==null&&(Qn(l,c),s&&aE(l)),ac()}}function Ay(e,n,t){let r=!1,o=n,i=e,a=k(null);try{for(;i!==null;){if(Lt(i)){i=i[Le];continue}let s=i[Oo];if(s)try{let c=e[nt][Me],l=c?.constructor,d={declarationInstance:c,declarationType:l,caughtBy:s},u=k(null);try{s(Lg(o),d)}finally{k(u)}r=!0;break}catch(c){o=c}i=i[Le]}}finally{k(a)}if(!r)throw t||$r(e),o}function Ry(e,n){for(let t=Ub(e);t!==null;t=Hb(t))for(let r=Ne;r<t.length;r++){let o=t[r];Oy(o,n)}}function mE(e){for(let n=Ub(e);n!==null;n=Hb(n)){if(!(n[F]&2))continue;let t=n[Hr];for(let r=0;r<t.length;r++){let o=t[r];Iu(o)}}}function pE(e,n,t){he(ae.ComponentStart);let r=jt(n,e);try{Oy(r,t)}finally{he(ae.ComponentEnd,r[Me])}}function Oy(e,n){ec(e)&&vf(e,n)}function vf(e,n){let r=e[O],o=e[F],i=e[Dt],a=!!(n===0&&o&16);if(a||=!!(o&64&&n===0),a||=!!(o&1024),a||=!!(i?.dirty&&bo(i)),a||=!1,i&&(i.dirty=!1),e[F]&=-9217,a)fE(r,e,r.template,e[Me]);else if(o&8192){let s=k(null);try{try{Ny(e)}catch(l){Ay(e,l,!1)}Ry(e,1);let c=r.components;c!==null&&Fy(e,c,1),Iy(e)}finally{k(s)}}}function Fy(e,n,t){for(let r=0;r<n.length;r++)pE(e,n[r],t)}function hE(e,n){let t=e.hostBindingOpCodes;if(t!==null)try{for(let r=0;r<t.length;r++){let o=t[r];if(o<0)cr(~o);else{let i=o,a=t[++r],s=t[++r];wg(a,i);let c=n[i];he(ae.HostBindingsUpdateStart,c);try{s(2,c)}finally{he(ae.HostBindingsUpdateEnd,c)}}}}finally{cr(-1)}}function om(e,n){let t=Vu()?64:1088;for(e[nn].changeDetectionScheduler?.notify(n);e;){e[F]|=t;let r=Vn(e);if(Fo(e)&&!r)return e;e=r}return null}function Py(e,n,t,r){return[e,!0,0,n,null,r,null,t,null,null]}function Ly(e,n){let t=Ne+n;if(t<e.length)return e[t]}function da(e,n,t,r=!0){let o=n[O];if(gE(o,n,e,t),r){let a=bf(t,e),s=n[De],c=s.parentNode(e[Ur]);c!==null&&Mw(o,e[Ze],s,n,c,a)}let i=n[Vr];i!==null&&i.firstChild!==null&&(i.firstChild=null)}function Vy(e,n){let t=ra(e,n);return t!==void 0&&Vc(t[O],t),t}function ra(e,n){if(e.length<=Ne)return;let t=Ne+n,r=e[t];if(r){let o=r[sr];o!==null&&o!==e&&Zf(o,r),n>0&&(e[t-1][Pt]=r[Pt]);let i=Hi(e,Ne+n);kw(r[O],r);let a=i[yn];a!==null&&a.detachView(i[O]),r[Le]=null,r[Pt]=null,r[F]&=-129}return r}function gE(e,n,t,r){let o=Ne+r,i=t.length;r>0&&(t[o-1][Pt]=n),r<i-Ne?(n[Pt]=t[o],hu(t,Ne+r,n)):(t.push(n),n[Pt]=null),n[Le]=t;let a=n[sr];a!==null&&t!==a&&jy(a,n);let s=n[yn];s!==null&&s.insertView(e),tc(n),n[F]|=128}function jy(e,n){let t=e[Hr],r=n[Le];if(Un(r))e[F]|=2;else{let o=r[Le][nt];n[nt]!==o&&(e[F]|=2)}t===null?e[Hr]=[n]:t.push(n)}var dr=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,t=n[O];return na(t,n,t.firstChild,[])}constructor(n,t){this._lView=n,this._cdRefInjectingView=t}get context(){return this._lView[Me]}set context(n){this._lView[Me]=n}get destroyed(){return zr(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[Le];if(Lt(n)){let t=n[Gi],r=t?t.indexOf(this):-1;r>-1&&(ra(n,r),Hi(t,r))}this._attachedToViewContainer=!1}Vc(this._lView[O],this._lView)}onDestroy(n){nc(this._lView,n)}markForCheck(){om(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[F]&=-129}reattach(){tc(this._lView),this._lView[F]|=128}detectChanges(){this._lView[F]|=1024,Ty(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new I(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=Fo(this._lView),t=this._lView[sr];t!==null&&!n&&Zf(t,this._lView),fy(this._lView[O],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new I(902,!1);this._appRef=n;let t=Fo(this._lView),r=this._lView[sr];r!==null&&!t&&jy(r,this._lView),tc(this._lView)}};var $o=(()=>{class e{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=bE;constructor(t,r,o){this._declarationLView=t,this._declarationTContainer=r,this.elementRef=o}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(t,r){return this.createEmbeddedViewImpl(t,r)}createEmbeddedViewImpl(t,r,o){let i=la(this._declarationLView,this._declarationTContainer,t,{embeddedViewInjector:r,dehydratedView:o});return new dr(i)}}return e})();function bE(){return Hc(rt(),z())}function Hc(e,n){return e.type&4?new $o(n,e,Ko(e,n)):null}function Zo(e,n,t,r,o){let i=e.data[n];if(i===null)i=yE(e,n,t,r,o),Sg()&&(i.flags|=32);else if(i.type&64){i.type=t,i.value=r,i.attrs=o;let a=_g();i.injectorIndex=a===null?-1:a.injectorIndex}return Po(i,!0),i}function yE(e,n,t,r,o){let i=Fu(),a=Pu(),s=a?i:i&&i.parent,c=e.data[n]=_E(e,s,t,n,r,o);return vE(e,c,i,a),c}function vE(e,n,t,r){e.firstChild===null&&(e.firstChild=n),t!==null&&(r?t.child==null&&n.parent!==null&&(t.child=n):t.next===null&&(t.next=n,n.prev=t))}function _E(e,n,t,r,o,i){let a=n?n.injectorIndex:-1,s=0;return Au()&&(s|=128),{type:t,index:r,insertBeforeIndex:null,injectorIndex:a,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:s,providerIndexes:0,value:o,namespace:Hu(),attrs:i,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function xE(e){let n=e[Su]??[],r=e[Le][De],o=[];for(let i of n)i.data[Wb]!==void 0?o.push(i):CE(i,r);e[Su]=o}function CE(e,n){let t=0,r=e.firstChild;if(r){let o=e.data[Gb];for(;t<o;){let i=r.nextSibling;ny(n,r,!1),r=i,t++}}}var SE=()=>null,wE=()=>null;function Dc(e,n){return SE(e,n)}function By(e,n,t){return wE(e,n,t)}var Uy=class{},It=class{},Qe=class{destroyNode=null;static __NG_ELEMENT_ID__=()=>EE()};function EE(){let e=z(),n=rt(),t=jt(n.index,e);return(Un(t)?t:e)[De]}var Hy=(()=>{class e{static \u0275prov=G({token:e,providedIn:"root",factory:()=>null})}return e})();function zy(e){return e.debugInfo?.className||e.type.name||null}var yc={},Ic=class{injector;parentInjector;constructor(n,t){this.injector=n,this.parentInjector=t}get(n,t,r){let o=this.injector.get(n,yc,r);return o!==yc||t===yc?o:this.parentInjector.get(n,t,r)}};function wn(e,n,t){if(t===ut)return!1;let r=e[n];return Object.is(r,t)?!1:(e[n]=t,!0)}function _f(e,n,t,r){let o=wn(e,n,t);return wn(e,n+1,r)||o}function DE(e,n,t,r,o,i){let a=_f(e,n,t,r);return _f(e,n+2,o,i)||a}function Ju(e,n,t){return function r(o){let i=r.__ngNativeEl__;i!==void 0&&KS(o,i);let a=Hn(e)?jt(e.index,n):n;om(a,5);let s=n[Me],c=ob(n,s,t,o),l=r.__ngNextListenerFn__;for(;l;)c=ob(n,s,l,o)&&c,l=l.__ngNextListenerFn__;return c}}function ob(e,n,t,r){let o=k(null);try{return he(ae.OutputStart,n,t),t(r)!==!1}catch(i){return eE(e,i),!1}finally{he(ae.OutputEnd,n,t),k(o)}}function IE(e,n,t,r,o,i,a,s){let c=Wi(e),l=!1,d=null;if(!r&&c&&(d=ME(n,t,i,e.index)),d!==null){let u=d.__ngLastListenerFn__||d;u.__ngNextListenerFn__=a,d.__ngLastListenerFn__=a,l=!0}else{let u=Vt(e,t),g=r?r(u):u;YS(t,g,i,s),r||(s.__ngNativeEl__=u);let h=o.listen(g,i,s);if(!kE(i)){let _=r?D=>r(Ye(D[e.index])):e.index;$y(_,n,t,i,s,h,!1)}}return l}function kE(e){return e.startsWith("animation")||e.startsWith("transition")}function ME(e,n,t,r){let o=e.cleanup;if(o!=null)for(let i=0;i<o.length-1;i+=2){let a=o[i];if(a===t&&o[i+1]===r){let s=n[Ao],c=o[i+2];return s&&s.length>c?s[c]:null}typeof a=="string"&&(i+=2)}return null}function $y(e,n,t,r,o,i,a){let s=n.firstCreatePass?Mu(n):null,c=ku(t),l=c.length;c.push(o,i),s&&s.push(r,e,l,(l+1)*(a?-1:1))}function ib(e,n,t,r,o,i){let a=n[t],s=n[O],l=s.data[t].outputs[r],u=a[l].subscribe(i);$y(e.index,s,n,o,i,u,!0)}var xf=Symbol("BINDING");var to=new C("");function kc(e,n,t){let r=t?e.styles:null,o=t?e.classes:null,i=0;if(n!==null)for(let a=0;a<n.length;a++){let s=n[a];if(typeof s=="number")i=s;else if(i==1)o=zs(o,s);else if(i==2){let c=s,l=n[++a];r=zs(r,c+": "+l+";")}}t?e.styles=r:e.stylesWithoutHost=r,t?e.classes=o:e.classesWithoutHost=o}function Ie(e,n=0){let t=z();if(t===null)return H(e,n);let r=rt();return Fb(r,t,qe(e),n)}function Gy(e,n,t,r,o){let i=r===null?null:{"":-1},a=o(e,t);if(a!==null){let s=a,c=null,l=null;for(let d of a)if(d.resolveHostDirectives!==null){[s,c,l]=d.resolveHostDirectives(a);break}AE(e,n,t,s,i,c,l)}i!==null&&r!==null&&NE(t,r,i)}function NE(e,n,t){let r=e.localNames=[];for(let o=0;o<n.length;o+=2){let i=t[n[o+1]];if(i==null)throw new I(-301,!1);r.push(n[o],i)}}function TE(e,n,t){n.componentOffset=t,(e.components??=[]).push(n.index)}function AE(e,n,t,r,o,i,a){let s=r.length,c=null;for(let g=0;g<s;g++){let h=r[g];c===null&&vn(h)&&(c=h,TE(e,t,g)),df(Cc(t,n),e,h.type)}VE(t,e.data.length,s),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let g=0;g<s;g++){let h=r[g];h.providersResolver&&h.providersResolver(h)}let l=!1,d=!1,u=yy(e,n,s,null);s>0&&(t.directiveToIndex=new Map);for(let g=0;g<s;g++){let h=r[g];if(t.mergedAttrs=Uo(t.mergedAttrs,h.hostAttrs),OE(e,t,n,u,h),LE(u,h,o),a!==null&&a.has(h)){let[D,A]=a.get(h);t.directiveToIndex.set(h.type,[u,D+t.directiveStart,A+t.directiveStart])}else(i===null||!i.has(h))&&t.directiveToIndex.set(h.type,u);h.contentQueries!==null&&(t.flags|=4),(h.hostBindings!==null||h.hostAttrs!==null||h.hostVars!==0)&&(t.flags|=64);let _=h.type.prototype;!l&&(_.ngOnChanges||_.ngOnInit||_.ngDoCheck)&&((e.preOrderHooks??=[]).push(t.index),l=!0),!d&&(_.ngOnChanges||_.ngDoCheck)&&((e.preOrderCheckHooks??=[]).push(t.index),d=!0),u++}RE(e,t,i)}function RE(e,n,t){for(let r=n.directiveStart;r<n.directiveEnd;r++){let o=e.data[r];if(t===null||!t.has(o))ab(0,n,o,r),ab(1,n,o,r),cb(n,r,!1);else{let i=t.get(o);sb(0,n,i,r),sb(1,n,i,r),cb(n,r,!0)}}}function ab(e,n,t,r){let o=e===0?t.inputs:t.outputs;for(let i in o)if(Object.hasOwn(o,i)){let a;e===0?a=n.inputs??={}:a=n.outputs??={},a[i]??=[],a[i].push(r),Wy(n,i)}}function sb(e,n,t,r){let o=e===0?t.inputs:t.outputs;for(let i in o)if(Object.hasOwn(o,i)){let a=o[i],s;e===0?s=n.hostDirectiveInputs??={}:s=n.hostDirectiveOutputs??={},s[a]??=[],s[a].push(r,i),Wy(n,a)}}function Wy(e,n){n==="class"?e.flags|=8:n==="style"&&(e.flags|=16)}function cb(e,n,t){let{attrs:r,inputs:o,hostDirectiveInputs:i}=e;if(r===null||!t&&o===null||t&&i===null||$f(e)){e.initialInputs??=[],e.initialInputs.push(null);return}let a=null,s=0;for(;s<r.length;){let c=r[s];if(c===0){s+=4;continue}else if(c===5){s+=2;continue}else if(typeof c=="number")break;if(!t&&Object.hasOwn(o,c)){let l=o[c];for(let d of l)if(d===n){a??=[],a.push(c,r[s+1]);break}}else if(t&&Object.hasOwn(i,c)){let l=i[c];for(let d=0;d<l.length;d+=2)if(l[d]===n){a??=[],a.push(l[d+1],r[s+1]);break}}s+=2}e.initialInputs??=[],e.initialInputs.push(a)}function OE(e,n,t,r,o){e.data[r]=o;let i=o.factory||(o.factory=Rr(o.type,!0)),a=new Yr(i,vn(o),Ie,null);e.blueprint[r]=a,t[r]=a,FE(e,n,r,yy(e,t,o.hostVars,ut),o)}function FE(e,n,t,r,o){let i=o.hostBindings;if(i){let a=e.hostBindingOpCodes;a===null&&(a=e.hostBindingOpCodes=[]);let s=~n.index;PE(a)!=s&&a.push(s),a.push(t,r,i)}}function PE(e){let n=e.length;for(;n>0;){let t=e[--n];if(typeof t=="number"&&t<0)return t}return 0}function LE(e,n,t){if(t){if(n.exportAs)for(let r=0;r<n.exportAs.length;r++)t[n.exportAs[r]]=e;vn(n)&&(t[""]=e)}}function VE(e,n,t){e.flags|=1,e.directiveStart=n,e.directiveEnd=n+t,e.providerIndexes=n}function qy(e,n,t,r,o,i,a,s){let c=n[O],l=c.consts,d=Bt(l,a),u=Zo(c,e,t,r,d);return i&&Gy(c,n,u,Bt(l,s),o),u.mergedAttrs=Uo(u.mergedAttrs,u.attrs),u.attrs!==null&&kc(u,u.attrs,!1),u.mergedAttrs!==null&&kc(u,u.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,u),u}function Ky(e,n){Db(e,n),wu(n)&&e.queries.elementEnd(n)}function jE(e,n,t,r,o,i){let a=n.consts,s=Bt(a,o),c=Zo(n,e,t,r,s);if(c.mergedAttrs=Uo(c.mergedAttrs,c.attrs),i!=null){let l=Bt(a,i);c.localNames=[];for(let d=0;d<l.length;d+=2)c.localNames.push(l[d],-1)}return c.attrs!==null&&kc(c,c.attrs,!1),c.mergedAttrs!==null&&kc(c,c.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,c),c}var Zy=typeof ShadowRoot<"u",BE=typeof Document<"u";function UE(e){return Object.keys(e).map(n=>{let[t,r,o]=e[n],i={propName:t,templateName:n,isSignal:(r&Bc.SignalBased)!==0};return o&&(i.transform=o),i})}function HE(e){return Object.keys(e).map(n=>({propName:e[n],templateName:n}))}function zE(e,n,t){let r=n instanceof Pe?n:n?.injector;return r&&e.getStandaloneInjector!==null&&(r=e.getStandaloneInjector(r)||r),r?new Ic(t,r):t}function $E(e){let n=e.get(It,null);if(n===null)throw new I(407,!1);let t=e.get(Hy,null),r=e.get(jn,null),o=e.get($n,null,{optional:!0});return{rendererFactory:n,sanitizer:t,changeDetectionScheduler:r,ngReflect:!1,tracingService:o}}function GE(e,n,t){let r=Yy(e);return ey(n,r,r==="svg"?Pr:r==="math"?Xs:t)}function WE(e){if((e&&"localName"in e&&typeof e.localName=="string"?e.localName:e?.tagName)?.toLowerCase()==="script")throw new I(905,!1)}function Yy(e){return(e.selectors[0][0]||"div").toLowerCase()}var Go=class{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=UE(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=HE(this.componentDef.outputs),this.cachedOutputs}constructor(n,t){this.componentDef=n,this.ngModule=t,this.componentType=n.type,this.selector=xw(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!t}create(n,t,r,o,i,a,s,c){he(ae.DynamicComponentStart);let l=k(null);try{let d=this.componentDef,u=zE(d,o||this.ngModule,n),g=$E(u),h=g.tracingService;return h&&h.componentCreate?h.componentCreate(zy(d),()=>this.createComponentRef(g,u,t,r,i,a,s,c)):this.createComponentRef(g,u,t,r,i,a,s,c)}finally{k(l)}}createComponentRef(n,t,r,o,i,a,s,c){let l=this.componentDef,d=qE(o,l,a,i,c),u=n.rendererFactory.createRenderer(null,l),g=o?zw(u,o,l.encapsulation,t):GE(l,u,s??null);WE(g);let h=t.get(to,null),_=KE(g,()=>t.get(ye,null)??$b());h&&h.addHost(_);let D=a?.some(lb)||i?.some(J=>typeof J!="function"&&J.bindings.some(lb)),A=Jf(null,d,null,512|by(l),null,null,n,u,t,null,Zb(g,t,!0));h&&Zy&&_ instanceof ShadowRoot&&nc(A,()=>{h.removeHost(_)}),A[Ve]=g,ic(A);let j=null;try{let J=qy(Ve,A,2,"#host",()=>d.directiveRegistry,!0,0);ry(u,g,J),Ho(g,A),tm(d,A,J),Qb(d,J,A),Ky(d,J),r!==void 0&&YE(J,this.ngContentSelectors,r),j=jt(J.index,A),A[Me]=j[Me],rm(d,A,null)}catch(J){throw j!==null&&ff(j),ff(A),J}finally{he(ae.DynamicComponentEnd),ac()}return new Mc(this.componentType,A,!!D)}};function qE(e,n,t,r,o){let i=e?["ng-version","22.2.6"]:Cw(n.selectors[0]),a=null,s=null,c=0;if(t)for(let u of t)c+=u[xf].requiredVars,u.create&&(u.targetIdx=0,(a??=[]).push(u)),u.update&&(u.targetIdx=0,(s??=[]).push(u));if(r)for(let u=0;u<r.length;u++){let g=r[u];if(typeof g!="function")for(let h of g.bindings){c+=h[xf].requiredVars;let _=u+1;h.create&&(h.targetIdx=_,(a??=[]).push(h)),h.update&&(h.targetIdx=_,(s??=[]).push(h))}}let l=[n];if(r)for(let u of r){let g=typeof u=="function"?u:u.type,h=Gs(g);l.push(h)}return Xf(0,null,ZE(a,s),1,c,l,null,null,null,[i],null)}function KE(e,n){let t=e.getRootNode?.();return BE&&t instanceof Document?t.head:t&&Zy&&t instanceof ShadowRoot?t:n().head}function ZE(e,n){return!e&&!n?null:t=>{if(t&1&&e)for(let r of e)r.create();if(t&2&&n)for(let r of n)r.update()}}function lb(e){let n=e[xf].kind;return n==="input"||n==="twoWay"}var Mc=class extends Uy{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,t,r){super(),this._rootLView=t,this._hasInputBindings=r,this._tNode=Js(t[O],Ve),this.location=Ko(this._tNode,t),this.instance=jt(this._tNode.index,t)[Me],this.hostView=this.changeDetectorRef=new dr(t,void 0),this.componentType=n}setInput(n,t){this._hasInputBindings;let r=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),t))return;let o=this._rootLView,i=nm(r,o[O],o,n,t);this.previousInputValues.set(n,t);let a=jt(r.index,o);om(a,1)}get injector(){return new zn(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function YE(e,n,t){let r=e.projection=[];for(let o=0;o<n.length;o++){let i=t[o];r.push(i!=null&&i.length?Array.from(i):null)}}var ur=(()=>{class e{static __NG_ELEMENT_ID__=QE}return e})();function QE(){let e=rt();return Qy(e,z())}var Cf=class e extends ur{_lContainer;_hostTNode;_hostLView;constructor(n,t,r){super(),this._lContainer=n,this._hostTNode=t,this._hostLView=r}get element(){return Ko(this._hostTNode,this._hostLView)}get injector(){return new zn(this._hostTNode,this._hostLView)}get parentInjector(){let n=jf(this._hostTNode,this._hostLView);if(Mb(n)){let t=xc(n,this._hostLView),r=_c(n),o=t[O].data[r+8];return new zn(o,t)}else return new zn(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let t=db(this._lContainer);return t!==null&&t[n]||null}get length(){return this._lContainer.length-Ne}createEmbeddedView(n,t,r){let o,i,a;typeof r=="number"?o=r:r!=null&&(o=r.index,i=r.injector,a=r.onError);let s=Dc(this._lContainer,n.ssrId),c=n.createEmbeddedViewImpl(t||{},i,s);return a&&(c._lView[Oo]=a),this.insertImpl(c,o,zo(this._hostTNode,s)),c}createComponent(n,t,r,o,i,a,s){let c,l,d=t||{};c=d.index,r=d.injector,o=d.projectableNodes,i=d.environmentInjector||d.ngModuleRef,a=d.directives,s=d.bindings,l=d.onError;let u=new Go(rr(n)),g=r||this.parentInjector;if(!i&&u.ngModule==null){let J=this.parentInjector.get(Pe,null);J&&(i=J)}let h=rr(u.componentType??{}),_=Dc(this._lContainer,h?.id??null),D=_?.firstChild??null,A=u.create(g,o,D,i,a,s,this._getHostElementNamespace());return l&&(A.hostView._lView[Oo]=l),this.insertImpl(A.hostView,c,zo(this._hostTNode,_)),A}_getHostElementNamespace(){if(this._hostTNode.type&2){let n=this._hostTNode.parent??this._hostLView[Ze];return n!==null&&n.type&2&&typeof n.value=="string"&&n.value.toLowerCase()==="foreignobject"?null:n?.namespace??null}return this._hostTNode.namespace}insert(n,t){return this.insertImpl(n,t,!0)}insertImpl(n,t,r){let o=n._lView;if(pg(o)){let s=this.indexOf(n);if(s!==-1)this.detach(s);else{let c=o[Le],l=new e(c,c[Ze],c[Le]);l.detach(l.indexOf(n))}}let i=this._adjustIndex(t),a=this._lContainer;return da(a,o,i,r),n.attachToViewContainerRef(),hu(ef(a),i,n),n}move(n,t){return this.insert(n,t)}indexOf(n){let t=db(this._lContainer);return t!==null?t.indexOf(n):-1}remove(n){let t=this._adjustIndex(n,-1),r=ra(this._lContainer,t);r&&(Hi(ef(this._lContainer),t),Vc(r[O],r))}detach(n){let t=this._adjustIndex(n,-1),r=ra(this._lContainer,t);return r&&Hi(ef(this._lContainer),t)!=null?new dr(r):null}_adjustIndex(n,t=0){return n??this.length+t}};function db(e){return e[Gi]}function ef(e){return e[Gi]||(e[Gi]=[])}function Qy(e,n){let t,r=n[e.index];return Lt(r)?t=r:(t=Py(r,n,null,e),n[e.index]=t,em(n,t)),JE(t,n,e,r),new Cf(t,e,n)}function XE(e,n){let t=e[De],r=t.createComment(""),o=Vt(n,e),i=t.parentNode(o);return Zr(t,i,r,t.nextSibling(o),!1),r}var JE=nD,eD=()=>!1;function tD(e,n,t){return eD(e,n,t)}function nD(e,n,t,r){if(e[Ur])return;let o;t.type&8?o=Ye(r):o=XE(n,t),e[Ur]=o}var Sf=class e{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new e(this.queryList)}setDirty(){this.queryList.setDirty()}},wf=class e{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let t=n.queries;if(t!==null){let r=n.contentQueries!==null?n.contentQueries[0]:t.length,o=[];for(let i=0;i<r;i++){let a=t.getByIndex(i),s=this.queries[a.indexInDeclarationView];o.push(s.clone())}return new e(o)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let t=0;t<this.queries.length;t++)am(n,t).matches!==null&&this.queries[t].setDirty()}},Nc=class{flags;read;predicate;constructor(n,t,r=null){this.flags=t,this.read=r,typeof n=="string"?this.predicate=sD(n):this.predicate=n}},Ef=class e{queries;constructor(n=[]){this.queries=n}elementStart(n,t){for(let r=0;r<this.queries.length;r++)this.queries[r].elementStart(n,t)}elementEnd(n){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(n)}embeddedTView(n){let t=null;for(let r=0;r<this.length;r++){let o=t!==null?t.length:0,i=this.getByIndex(r).embeddedTView(n,o);i&&(i.indexInDeclarationView=r,t!==null?t.push(i):t=[i])}return t!==null?new e(t):null}template(n,t){for(let r=0;r<this.queries.length;r++)this.queries[r].template(n,t)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},Df=class e{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,t=-1){this.metadata=n,this._declarationNodeIndex=t}elementStart(n,t){this.isApplyingToNode(t)&&this.matchTNode(n,t)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,t){this.elementStart(n,t)}embeddedTView(n,t){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,t),new e(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,r=n.parent;for(;r!==null&&r.type&8&&r.index!==t;)r=r.parent;return t===(r!==null?r.index:-1)}return this._appliesToNextNode}matchTNode(n,t){let r=this.metadata.predicate;if(Array.isArray(r))for(let o=0;o<r.length;o++){let i=r[o];this.matchTNodeWithReadOption(n,t,rD(t,i)),this.matchTNodeWithReadOption(n,t,bc(t,n,i,!1,!1))}else r===$o?t.type&4&&this.matchTNodeWithReadOption(n,t,-1):this.matchTNodeWithReadOption(n,t,bc(t,n,r,!1,!1))}matchTNodeWithReadOption(n,t,r){if(r!==null){let o=this.metadata.read;if(o!==null)if(o===ue||o===ur||o===xe||o===$o&&t.type&4)this.addMatch(t.index,-2);else{let i=bc(t,n,o,!1,!1);i!==null&&this.addMatch(t.index,i)}else this.addMatch(t.index,r)}}addMatch(n,t){this.matches===null?this.matches=[n,t]:this.matches.push(n,t)}};function rD(e,n){let t=e.localNames;if(t!==null){for(let r=0;r<t.length;r+=2)if(t[r]===n)return t[r+1]}return null}function oD(e,n){return e.type&11?Ko(e,n):e.type&4?Hc(e,n):null}function iD(e,n,t,r){return t===-1?oD(n,e):t===-2?aD(e,n,r):ta(e,e[O],t,n)}function aD(e,n,t){if(t===ue)return Ko(n,e);if(t===$o)return Hc(n,e);if(t===ur)return Qy(n,e);if(t===xe)return new zn(n,e)}function Xy(e,n,t,r){let o=n[yn].queries[r];if(o.matches===null){let i=e.data,a=t.matches,s=[];for(let c=0;a!==null&&c<a.length;c+=2){let l=a[c];if(l<0)s.push(null);else{let d=i[l];s.push(iD(n,d,a[c+1],t.metadata.read))}}o.matches=s}return o.matches}function If(e,n,t,r){let o=e.queries.getByIndex(t),i=o.matches;if(i!==null){let a=Xy(e,n,o,t);for(let s=0;s<i.length;s+=2){let c=i[s];if(c>0)r.push(a[s/2]);else{let l=i[s+1],d=n[-c];for(let u=Ne;u<d.length;u++){let g=d[u];g[sr]===g[Le]&&If(g[O],g,l,r)}if(d[Hr]!==null){let u=d[Hr];for(let g=0;g<u.length;g++){let h=u[g];If(h[O],h,l,r)}}}}}return r}function im(e,n){return e[yn].queries[n].queryList}function Jy(e,n,t){let r=new Sc((t&4)===4);return bg(e,n,r,r.destroy),(n[yn]??=new wf).queries.push(new Sf(r))-1}function ev(e,n,t){let r=Ue();return r.firstCreatePass&&(nv(r,new Nc(e,n,t),-1),(n&2)===2&&(r.staticViewQueries=!0)),Jy(r,z(),n)}function tv(e,n,t,r){let o=Ue();if(o.firstCreatePass){let i=rt();nv(o,new Nc(n,t,r),i.index),cD(o,e),(t&2)===2&&(o.staticContentQueries=!0)}return Jy(o,z(),t)}function sD(e){return e.split(",").map(n=>n.trim())}function nv(e,n,t){e.queries===null&&(e.queries=new Ef),e.queries.track(new Df(n,t))}function cD(e,n){let t=e.contentQueries||(e.contentQueries=[]),r=t.length?t[t.length-1]:-1;n!==r&&t.push(e.queries.length-1,n)}function am(e,n){return e.queries.getByIndex(n)}function rv(e,n){let t=e[O],r=am(t,n);return r.crossesNgTemplate?If(t,e,n,[]):Xy(t,e,r,n)}function ov(e,n,t){let r,o=Ii(()=>{r._dirtyCounter();let i=lD(r,e);if(n&&i===void 0)throw new I(-951,!1);return i});return r=o[Be],r._dirtyCounter=E(0),r._flatValue=void 0,o}function sm(e){return ov(!0,!1,e)}function cm(e){return ov(!0,!0,e)}function iv(e,n){let t=e[Be];t._lView=z(),t._queryIndex=n,t._queryList=im(t._lView,n),t._queryList.onDirty(()=>t._dirtyCounter.update(r=>r+1))}function lD(e,n){let t=e._lView,r=e._queryIndex;if(t===void 0||r===void 0||t[F]&4)return n?void 0:et;let o=im(t,r),i=rv(t,r);return o.reset(i,Vb),n?o.first:o._changesDetected||e._flatValue===void 0?e._flatValue=o.toArray():e._flatValue}function Gn(e){return!!e&&typeof e.then=="function"}function lm(e){return!!e&&typeof e.subscribe=="function"}var Xr=class{},zc=class{};var Tc=class extends Xr{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];constructor(n,t,r,o=!0){super(),this.ngModuleType=n,this._parent=t;let i=Kh(n);this._bootstrapComponents=dw(i.bootstrap),this._r3Injector=zu(n,t,[{provide:Xr,useValue:this},...r],Li(n),new Set(["environment"])),o&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(t=>t()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},Ac=class extends zc{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new Tc(this.moduleType,n,[])}};var oa=class extends Xr{injector;instance=null;constructor(n){super();let t=new Fr([...n.providers,{provide:Xr,useValue:this}],n.parent||To(),n.debugName,new Set(["environment"]));this.injector=t,n.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function ua(e,n,t=null){return new oa({providers:e,parent:n,debugName:t,runEnvironmentInitializers:!0}).injector}var dD=(()=>{class e{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let r=yu(!1,t.type),o=r.length>0?ua([r],this._injector,""):null;this.cachedInjectors.set(t,o)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=G({token:e,providedIn:"environment",factory:()=>new e(H(Pe))})}return e})();function Q(e){return sa(()=>{let n=av(e),t=T(v({},n),{decls:e.decls,vars:e.vars,template:e.template,consts:e.consts||null,ngContentSelectors:e.ngContentSelectors,onPush:e.changeDetection!==Bf.Eager,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&e.dependencies||null,getStandaloneInjector:n.standalone?o=>o.get(dD).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:e.signals??!1,data:e.data||{},encapsulation:e.encapsulation||an.Emulated,styles:e.styles||et,_:null,schemas:e.schemas||null,tView:null,id:""});n.standalone&&eo("NgStandalone"),sv(t);let r=e.dependencies;return t.directiveDefs=ub(r,uD),t.pipeDefs=ub(r,Zh),t.id=pD(t),t})}function uD(e){return rr(e)||Gs(e)}function ke(e){return sa(()=>({type:e.type,bootstrap:e.bootstrap||et,declarations:e.declarations||et,imports:e.imports||et,exports:e.exports||et,transitiveCompileScopes:null,schemas:e.schemas||null,id:e.id||null}))}function fD(e,n){if(e==null)return or;let t={};for(let r in e)if(Object.hasOwn(e,r)){let o=e[r],i,a,s,c;Array.isArray(o)?(s=o[0],i=o[1],a=o[2]??i,c=o[3]||null):(i=o,a=o,s=Bc.None,c=null),t[i]=[r,s,c],n[i]=a}return t}function mD(e){if(e==null)return or;let n={};for(let t in e)Object.hasOwn(e,t)&&(n[e[t]]=t);return n}function re(e){return sa(()=>{let n=av(e);return sv(n),n})}function av(e){let n={};return{type:e.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:e.hostBindings||null,hostVars:e.hostVars||0,hostAttrs:e.hostAttrs||null,contentQueries:e.contentQueries||null,declaredInputs:n,inputConfig:e.inputs||or,exportAs:e.exportAs||null,standalone:e.standalone??!0,signals:e.signals===!0,selectors:e.selectors||et,viewQuery:e.viewQuery||null,features:e.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,signalFormsInputPresence:null,inputs:fD(e.inputs,n),outputs:mD(e.outputs),debugInfo:null}}function sv(e){e.features?.forEach(n=>n(e))}function ub(e,n){return e?()=>{let t=typeof e=="function"?e():e,r=[];for(let o of t){let i=n(o);i!==null&&r.push(i)}return r}:null}function pD(e){let n=0,t=typeof e.consts=="function"?"":e.consts,r=[e.selectors,e.ngContentSelectors,e.hostVars,e.hostAttrs,t,e.vars,e.decls,e.encapsulation,e.standalone,e.signals,e.exportAs,JSON.stringify(e.inputs),JSON.stringify(e.outputs),Object.getOwnPropertyNames(e.type.prototype),!!e.contentQueries,!!e.viewQuery];for(let i of r.join("|"))n=Math.imul(31,n)+i.charCodeAt(0)<<0;return n+=2147483648,"c"+n}var cv=new C("");var dm=(()=>{class e{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,r)=>{this.resolve=t,this.reject=r});appInits=f(cv,{optional:!0})??[];injector=f(xe);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let o of this.appInits){let i=Ke(this.injector,o);if(Gn(i))t.push(i);else if(lm(i)){let a=new Promise((s,c)=>{i.subscribe({complete:s,error:c})});t.push(a)}}let r=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{r()}).catch(o=>{this.reject(o)}),t.length===0&&r(),this.initialized=!0}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})();function hD(e){return Object.getPrototypeOf(e.prototype).constructor}function kt(e){let n=hD(e.type),t=!0,r=[e];for(;n&&n!==Function.prototype&&n!==Object.prototype;){let o,i=Object.hasOwn(n,ji)?n[ji]:void 0,a=Object.hasOwn(n,Bi)?n[Bi]:void 0;if(vn(e))o=i??a;else{if(i)throw new I(903,!1);o=a}if(o){if(t){r.push(o);let c=e;c.inputs=tf(e.inputs),c.declaredInputs=tf(e.declaredInputs),c.outputs=tf(e.outputs);let l=o.hostBindings;l&&_D(e,l);let d=o.viewQuery,u=o.contentQueries;if(d&&yD(e,d),u&&vD(e,u),gD(e,o),qh(e.outputs,o.outputs),vn(o)&&o.data.animation){let g=e.data;g.animation=(g.animation||[]).concat(o.data.animation)}}let s=o.features;if(s)for(let c=0;c<s.length;c++){let l=s[c];l&&l.ngInherit&&l(e),l===kt&&(t=!1)}}n=Object.getPrototypeOf(n)}bD(r)}function gD(e,n){for(let t in n.inputs){if(!Object.hasOwn(n.inputs,t)||Object.hasOwn(e.inputs,t))continue;let r=n.inputs[t];r!==void 0&&(e.inputs[t]=r,e.declaredInputs[t]=n.declaredInputs[t])}}function bD(e){let n=0,t=null;for(let r=e.length-1;r>=0;r--){let o=e[r];o.hostVars=n+=o.hostVars,o.hostAttrs=Uo(o.hostAttrs,t=Uo(t,o.hostAttrs))}}function tf(e){return e===or?{}:e===et?[]:e}function yD(e,n){let t=e.viewQuery;t?e.viewQuery=(r,o)=>{n(r,o),t(r,o)}:e.viewQuery=n}function vD(e,n){let t=e.contentQueries;t?e.contentQueries=(r,o,i)=>{n(r,o,i),t(r,o,i)}:e.contentQueries=n}function _D(e,n){let t=e.hostBindings;t?e.hostBindings=(r,o)=>{n(r,o),t(r,o)}:e.hostBindings=n}function lv(e,n,t,r,o,i,a,s){if(t.firstCreatePass){e.mergedAttrs=Uo(e.mergedAttrs,e.attrs);let d=e.tView=Xf(2,e,o,i,a,t.directiveRegistry,t.pipeRegistry,null,t.schemas,t.consts,null);t.queries!==null&&(t.queries.template(t,e),d.queries=t.queries.embeddedTView(e))}s&&(e.flags|=s),Po(e,!1);let c=CD(t,n,e,r);cc()&&Yf(t,n,c,e),Ho(c,n);let l=Py(c,n,c,e);n[r+Ve]=l,em(n,l),tD(l,e,n)}function xD(e,n,t,r,o,i,a,s,c,l,d){let u=t+Ve,g;return n.firstCreatePass?(g=Zo(n,u,4,a||null,s||null),Tu()&&Gy(n,e,g,Bt(n.consts,l),Sy),Db(n,g)):g=n.data[u],lv(g,e,n,t,r,o,i,c),Wi(g)&&tm(n,e,g),l!=null&&Uc(e,g,d),g}function ia(e,n,t,r,o,i,a,s,c,l,d){let u=t+Ve,g;if(n.firstCreatePass){if(g=Zo(n,u,4,a||null,s||null),l!=null){let h=Bt(n.consts,l);g.localNames=[];for(let _=0;_<h.length;_+=2)g.localNames.push(h[_],-1)}}else g=n.data[u];return lv(g,e,n,t,r,o,i,c),l!=null&&Uc(e,g,d),g}function Yo(e,n,t,r,o,i,a,s){let c=z(),l=Ue(),d=Bt(l.consts,i);return xD(c,l,e,n,t,r,o,d,void 0,a,s),Yo}var CD=SD;function SD(e,n,t,r){return lc(!0),n[De].createComment("")}var um=(()=>{class e{log(t){console.log(t)}warn(t){console.warn(t)}static \u0275fac=function(r){return new(r||e)};static \u0275prov=G({token:e,factory:e.\u0275fac,providedIn:"platform"})}return e})();var fm=new C("");var $c=new C("");function dv(){_d(e=>{let n="";throw new I(-600,n)})}var wD=10;var Wn=(()=>{class e{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=f(Ht);afterRenderManager=f(qf);zonelessEnabled=f(Xi);rootEffectScheduler=f(fc);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new q;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=f(lr);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(be(t=>!t))}constructor(){f($n,{optional:!0})}whenStable(){let t;return new Promise(r=>{t=this.isStable.subscribe({next:o=>{o&&r()}})}).finally(()=>{t.unsubscribe()})}_injector=f(Pe);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,r){return this.bootstrapImpl(t,r)}bootstrapImpl(t,r,o=xe.NULL){return this._injector.get(Y).run(()=>{if(he(ae.BootstrapComponentStart),!this._injector.get(dm).done){let j="";throw new I(405,j)}let s=rr(t),c=this._injector.get(Xr),l=new Go(s,c);this.componentTypes.push(t);let{hostElement:d,directives:u,bindings:g}=ED(r),h=d||l.selector,_=l.create(o,[],h,c.injector,u,g),D=_.location.nativeElement,A=_.injector.get(fm,null);return A?.registerApplication(D),_.onDestroy(()=>{this.detachView(_.hostView),ea(this.components,_),A?.unregisterApplication(D)}),this._loadComponent(_),he(ae.BootstrapComponentEnd,_),_})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){he(ae.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Lc.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw he(ae.ChangeDetectionEnd),new I(101,!1);let t=k(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,k(t),this.afterTick.next(),he(ae.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(It,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<wD;){he(ae.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{he(ae.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let t=!1;if(this.dirtyFlags&7){let r=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:o}of this.allViews){if(!r&&!qi(o))continue;let i=r&&!this.zonelessEnabled?0:1;Ty(o,i),t=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}t||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>qi(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let r=t;this._views.push(r),r.attachToAppRef(this)}detachView(t){let r=t;ea(this._views,r),r.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView);try{this.tick()}catch(o){this.internalErrorHandler(o)}this.components.push(t),this._injector.get($c,[]).forEach(o=>o(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>ea(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new I(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})();function ED(e){return e===void 0||typeof e=="string"||e instanceof Element?{hostElement:e}:e}function ea(e,n){let t=e.indexOf(n);t>-1&&e.splice(t,1)}function ge(e,n,t,r){let o=z(),i=Gr();if(wn(o,i,n)){let a=Ue(),s=Yi();Xw(s,o,e,n,t,r)}return ge}var kf=class{destroy(n){}updateValue(n,t){}swap(n,t){let r=Math.min(n,t),o=Math.max(n,t),i=this.detach(o);if(o-r>1){let a=this.detach(r);this.attach(r,i),this.attach(o,a)}else this.attach(r,i)}move(n,t){this.attach(t,this.detach(n))}};function nf(e,n,t,r,o){return e===t&&Object.is(n,r)?1:Object.is(o(e,n),o(t,r))?-1:0}function DD(e,n,t,r){let o,i,a=0,s=e.length-1,c=void 0;if(Array.isArray(n)){k(r);let l=n.length-1;for(k(null);a<=s&&a<=l;){let d=e.at(a),u=n[a],g=nf(a,d,a,u,t);if(g!==0){g<0&&e.updateValue(a,u),a++;continue}let h=e.at(s),_=n[l],D=nf(s,h,l,_,t);if(D!==0){D<0&&e.updateValue(s,_),s--,l--;continue}let A=t(a,d),j=t(s,h),J=t(a,u);if(Object.is(J,j)){let Je=t(l,_);Object.is(Je,A)?(e.swap(a,s),e.updateValue(s,_),l--,s--):e.move(s,a),e.updateValue(a,u),a++;continue}if(o??=new Rc,i??=mb(e,a,s,t),Mf(e,o,a,J))e.updateValue(a,u),a++,s++;else if(i.has(J))o.set(A,e.detach(a)),s--;else{let Je=e.create(a,n[a]);e.attach(a,Je),a++,s++}}for(;a<=l;)fb(e,o,t,a,n[a]),a++}else if(n!=null){k(r);let l=n[Symbol.iterator]();k(null);let d=l.next();for(;!d.done&&a<=s;){let u=e.at(a),g=d.value,h=nf(a,u,a,g,t);if(h!==0)h<0&&e.updateValue(a,g),a++,d=l.next();else{o??=new Rc,i??=mb(e,a,s,t);let _=t(a,g);if(Mf(e,o,a,_))e.updateValue(a,g),a++,s++,d=l.next();else if(!i.has(_))e.attach(a,e.create(a,g)),a++,s++,d=l.next();else{let D=t(a,u);o.set(D,e.detach(a)),s--}}}for(;!d.done;)fb(e,o,t,e.length,d.value),d=l.next()}for(;a<=s;)e.destroy(e.detach(s--));o?.forEach(l=>{e.destroy(l)})}function Mf(e,n,t,r){return n!==void 0&&n.has(r)?(e.attach(t,n.get(r)),n.delete(r),!0):!1}function fb(e,n,t,r,o){if(Mf(e,n,r,t(r,o)))e.updateValue(r,o);else{let i=e.create(r,o);e.attach(r,i)}}function mb(e,n,t,r){let o=new Set;for(let i=n;i<=t;i++)o.add(r(i,e.at(i)));return o}var Rc=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let t=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(t)?(this.kvMap.set(n,this._vMap.get(t)),this._vMap.delete(t)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,t){if(this.kvMap.has(n)){let r=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let o=this._vMap;for(;o.has(r);)r=o.get(r);o.set(r,t)}else this.kvMap.set(n,t)}forEach(n){for(let[t,r]of this.kvMap)if(n(r,t),this._vMap!==void 0){let o=this._vMap;for(;o.has(r);)r=o.get(r),n(r,t)}}};function M(e,n,t,r,o,i,a,s){eo("NgControlFlow");let c=z(),l=Ue(),d=Bt(l.consts,i);return ia(c,l,e,n,t,r,o,d,256,a,s),mm}function mm(e,n,t,r,o,i,a,s){return eo("NgControlFlow"),ID(e,n,t,r,o,i,a,s),mm}function ID(e,n,t,r,o,i,a,s){let c=z(),l=Ue(),d=Bt(l.consts,i);ia(c,l,e,n,t,r,o,d,512,a,s)}function N(e,n){eo("NgControlFlow");let t=z(),r=Gr(),o=t[r]!==ut?t[r]:-1,i=o!==-1?Oc(t,Ve+o):void 0,a=0;if(wn(t,r,e)){let s=k(null);try{if(i!==void 0&&Vy(i,a),e!==-1){let c=Ve+e,l=Oc(t,c),d=Rf(t[O],c),u=By(l,d,t),g=la(t,d,n,{dehydratedView:u});da(l,g,a,zo(d,u))}}finally{k(s)}}else if(i!==void 0){let s=Ly(i,a);s!==void 0&&(s[Me]=n)}}var Nf=class{lContainer;$implicit;$index;constructor(n,t,r){this.lContainer=n,this.$implicit=t,this.$index=r}get $count(){return this.lContainer.length-Ne}};var Tf=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,t,r){this.hasEmptyBlock=n,this.trackByFn=t,this.liveCollection=r}};function Qo(e,n,t,r,o,i,a,s,c,l,d,u,g){eo("NgControlFlow");let h=z(),_=Ue(),D=c!==void 0,A=z(),j=s?a.bind(A[nt][Me]):a,J=new Tf(D,j);A[Ve+e]=J,ia(h,_,e+1,n,t,r,o,Bt(_.consts,i),256),D&&ia(h,_,e+2,c,l,d,u,Bt(_.consts,g),512)}var Af=class extends kf{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,t,r){super(),this.lContainer=n,this.hostLView=t,this.templateTNode=r}get length(){return this.lContainer.length-Ne}at(n){return this.getLView(n)[Me].$implicit}attach(n,t){let r=t[Vr];this.needsIndexUpdate||=n!==this.length,da(this.lContainer,t,n,zo(this.templateTNode,r)),kD(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,MD(this.lContainer,n),ND(this.lContainer,n)}create(n,t){let r=Dc(this.lContainer,this.templateTNode.tView.ssrId);return la(this.hostLView,this.templateTNode,new Nf(this.lContainer,t,n),{dehydratedView:r})}destroy(n){Vc(n[O],n)}updateValue(n,t){this.getLView(n)[Me].$implicit=t}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[Me].$index=n}getLView(n){return TD(this.lContainer,n)}};function Xo(e){let n=k(null),t=Ut();try{let r=z(),o=r[O],i=r[t],a=t+1,s=Oc(r,a);if(i.liveCollection===void 0){let l=Rf(o,a);i.liveCollection=new Af(s,r,l)}else i.liveCollection.reset();let c=i.liveCollection;if(DD(c,e,i.trackByFn,n),c.updateIndexes(),i.hasEmptyBlock){let l=Gr(),d=c.length===0;if(wn(r,l,d)){let u=t+2,g=Oc(r,u);if(d){let h=Rf(o,u),_=By(g,h,r),D=la(r,h,void 0,{dehydratedView:_});da(g,D,0,zo(h,_))}else o.firstUpdatePass&&xE(g),Vy(g,0)}}}finally{k(n)}}function Oc(e,n){return e[n]}function kD(e,n){if(e.length<=Ne)return;let t=Ne+n,r=e[t];r&&Iw(r)}function MD(e,n){if(e.length<=Ne)return;let t=Ne+n,r=e[t];r&&Dw(r)}function ND(e,n){return ra(e,n)}function TD(e,n){return Ly(e,n)}function Rf(e,n){return Js(e,n)}function R(e,n,t){let r=z(),o=Gr();if(wn(r,o,n)){let i=Ue(),a=Yi();qw(a,r,e,n,r[De],t)}return R}function Of(e,n,t,r,o){nm(n,e,t,o?"class":"style",r)}function p(e,n,t,r){let o=z(),i=o[O],a=e+Ve,s=i.firstCreatePass?qy(a,o,2,n,Sy,Tu(),t,r):i.data[a];if(Hn(s)){let c=o[nn].tracingService;if(c&&c.componentCreate){let l=i.data[s.directiveStart+s.componentOffset];return c.componentCreate(zy(l),()=>(pb(e,n,o,s,r),p))}}return pb(e,n,o,s,r),p}function pb(e,n,t,r,o){if(wy(r,t,e,n,uv),Wi(r)){let i=t[O];tm(i,t,r),Qb(i,r,t)}o!=null&&Uc(t,r)}function m(){let e=Ue(),n=rt(),t=Ey(n);return e.firstCreatePass&&Ky(e,t),Ru(t)&&Ou(),Nu(),t.classesWithoutHost!=null&&NS(t)&&Of(e,t,z(),t.classesWithoutHost,!0),t.stylesWithoutHost!=null&&TS(t)&&Of(e,t,z(),t.stylesWithoutHost,!1),m}function oe(e,n,t,r){return p(e,n,t,r),m(),oe}function zt(e,n,t,r){let o=z(),i=o[O],a=e+Ve,s=i.firstCreatePass?jE(a,i,2,n,t,r):i.data[a];return wy(s,o,e,n,uv),r!=null&&Uc(o,s),zt}function $t(){let e=rt(),n=Ey(e);return Ru(n)&&Ou(),Nu(),$t}function Mt(e,n,t,r){return zt(e,n,t,r),$t(),Mt}var uv=(e,n,t,r,o)=>(lc(!0),ey(n[De],r,Hu()));function fe(){return z()}function fr(e,n,t){let r=z(),o=Gr();if(wn(r,o,n)){let i=Ue(),a=Yi();xy(a,r,e,n,r[De],t)}return fr}var fa="en-US";var AD=fa;function fv(e){typeof e=="string"&&(AD=e.toLowerCase().replace(/_/g,"-"))}function w(e,n,t){let r=z(),o=Ue(),i=rt();return RD(o,r,r[De],i,e,n,t),w}function RD(e,n,t,r,o,i,a){let s=!0,c=null;if((r.type&3||a)&&(c??=Ju(r,n,i),IE(r,e,n,a,t,o,i,c)&&(s=!1)),s){let l=r.outputs?.[o],d=r.hostDirectiveOutputs?.[o];if(d&&d.length)for(let u=0;u<d.length;u+=2){let g=d[u],h=d[u+1];c??=Ju(r,n,i),ib(r,n,g,h,o,c)}if(l&&l.length)for(let u of l)c??=Ju(r,n,i),ib(r,n,u,o,o,c)}}function x(e=1){return Ng(e)}function OD(e,n){let t=null,r=gw(e);for(let o=0;o<n.length;o++){let i=n[o];if(i==="*"){t=o;continue}if(r===null?cy(e,i,!0):vw(r,i))return o}return t}function vt(e){let n=z()[nt][Ze];if(!n.projection){let t=e?e.length:1,r=n.projection=tg(t,null),o=r.slice(),i=n.child;for(;i!==null;){if(i.type!==128){let a=e?OD(i,e):0;a!==null&&(o[a]?o[a].projectionNext=i:r[a]=i,o[a]=i)}i=i.next}}}function Ce(e,n=0,t,r,o,i){let a=z(),s=Ue(),c=r?e+1:null;c!==null&&ia(a,s,c,r,o,i,null,t);let l=Zo(s,Ve+e,16,null,t||null);l.projection===null&&(l.projection=n),Lu();let u=!a[Vr]||Au();a[nt][Ze].projection[l.projection]===null&&c!==null?FD(a,s,c):u&&!Pc(l)&&Lw(s,a,l)}function FD(e,n,t){let r=Ve+t,o=n.data[r],i=e[r],a=Dc(i,o.tView.ssrId),s=la(e,o,void 0,{dehydratedView:a});da(i,s,0,zo(o,a))}function no(e,n,t,r){return tv(e,n,t,r),no}function qn(e,n,t){return ev(e,n,t),qn}function Te(e){let n=z(),t=Ue(),r=oc();Zi(r+1);let o=am(t,r);if(e.dirty&&mg(n)===((o.metadata.flags&2)===2)){if(o.matches===null)e.reset([]);else{let i=rv(n,r);e.reset(i,Vb),e.notifyOnChanges()}return!0}return!1}function Ae(){return im(z(),oc())}function Gc(e,n,t,r,o){return iv(n,tv(e,t,r,o)),Gc}function Wc(e,n,t,r){return iv(e,ev(n,t,r)),Wc}function qc(e=1){Zi(oc()+e)}function mr(e){let n=xg();return fg(n,Ve+e)}function pc(e,n){return e<<17|n<<2}function Jr(e){return e>>17&32767}function PD(e){return(e&2)==2}function LD(e,n){return e&131071|n<<17}function Ff(e){return e|2}function Wo(e){return(e&131068)>>2}function rf(e,n){return e&-131069|n<<2}function VD(e){return(e&1)===1}function Pf(e){return e|1}function jD(e,n,t,r,o,i){let a=i?n.classBindings:n.styleBindings,s=Jr(a),c=Wo(a);e[r]=t;let l=!1,d;if(Array.isArray(t)){let u=t;d=u[1],(d===null||Mo(u,d)>0)&&(l=!0)}else d=t;if(o)if(c!==0){let g=Jr(e[s+1]);e[r+1]=pc(g,s),g!==0&&(e[g+1]=rf(e[g+1],r)),e[s+1]=LD(e[s+1],r)}else e[r+1]=pc(s,0),s!==0&&(e[s+1]=rf(e[s+1],r)),s=r;else e[r+1]=pc(c,0),s===0?s=r:e[c+1]=rf(e[c+1],r),c=r;l&&(e[r+1]=Ff(e[r+1])),hb(e,d,r,!0),hb(e,d,r,!1),BD(n,d,e,r,i),a=pc(s,c),i?n.classBindings=a:n.styleBindings=a}function BD(e,n,t,r,o){let i=o?e.residualClasses:e.residualStyles;i!=null&&typeof n=="string"&&Mo(i,n)>=0&&(t[r+1]=Pf(t[r+1]))}function hb(e,n,t,r){let o=e[t+1],i=n===null,a=r?Jr(o):Wo(o),s=!1;for(;a!==0&&(s===!1||i);){let c=e[a],l=e[a+1];UD(c,n)&&(s=!0,e[a+1]=r?Pf(l):Ff(l)),a=r?Jr(l):Wo(l)}s&&(e[t+1]=r?Ff(o):Pf(o))}function UD(e,n){return e===null||n==null||(Array.isArray(e)?e[1]:e)===n?!0:Array.isArray(e)&&typeof n=="string"?Mo(e,n)>=0:!1}var on={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function HD(e){return e.substring(on.key,on.keyEnd)}function zD(e){return $D(e),mv(e,pv(e,0,on.textEnd))}function mv(e,n){let t=on.textEnd;return t===n?-1:(n=on.keyEnd=GD(e,on.key=n,t),pv(e,n,t))}function $D(e){on.key=0,on.keyEnd=0,on.value=0,on.valueEnd=0,on.textEnd=e.length}function pv(e,n,t){for(;n<t&&e.charCodeAt(n)<=32;)n++;return n}function GD(e,n,t){for(;n<t&&e.charCodeAt(n)>32;)n++;return n}function ce(e,n){return qD(e,n,null,!0),ce}function ro(e){KD(eI,WD,e,!0)}function WD(e,n){for(let t=zD(n);t>=0;t=mv(n,t))Zs(e,HD(n),!0)}function qD(e,n,t,r){let o=z(),i=Ue(),a=Ki(2);if(i.firstUpdatePass&&gv(i,e,a,r),n!==ut&&wn(o,a,n)){let s=i.data[Ut()];bv(i,s,o,o[De],e,o[a+1]=nI(n,t),r,a)}}function KD(e,n,t,r){let o=Ue(),i=Ki(2);o.firstUpdatePass&&gv(o,null,i,r);let a=z();if(t!==ut&&wn(a,i,t)){let s=o.data[Ut()];if(yv(s,r)&&!hv(o,i)){let c=r?s.classesWithoutHost:s.stylesWithoutHost;c!==null&&(t=zs(c,t||"")),Of(o,s,a,t,r)}else tI(o,s,a,a[De],a[i+1],a[i+1]=JD(e,n,t),r,i)}}function hv(e,n){return n>=e.expandoStartIndex}function gv(e,n,t,r){let o=e.data;if(o[t+1]===null){let i=o[Ut()],a=hv(e,t);yv(i,r)&&n===null&&!a&&(n=!1),n=ZD(o,i,n,r),jD(o,i,n,t,a,r)}}function ZD(e,n,t,r){let o=Dg(e),i=r?n.residualClasses:n.residualStyles;if(o===null)(r?n.classBindings:n.styleBindings)===0&&(t=of(null,e,n,t,r),t=aa(t,n.attrs,r),i=null);else{let a=n.directiveStylingLast;if(a===-1||e[a]!==o)if(t=of(o,e,n,t,r),i===null){let c=YD(e,n,r);c!==void 0&&Array.isArray(c)&&(c=of(null,e,n,c[1],r),c=aa(c,n.attrs,r),QD(e,n,r,c))}else i=XD(e,n,r)}return i!==void 0&&(r?n.residualClasses=i:n.residualStyles=i),t}function YD(e,n,t){let r=t?n.classBindings:n.styleBindings;if(Wo(r)!==0)return e[Jr(r)]}function QD(e,n,t,r){let o=t?n.classBindings:n.styleBindings;e[Jr(o)]=r}function XD(e,n,t){let r,o=n.directiveEnd;for(let i=1+n.directiveStylingLast;i<o;i++){let a=e[i].hostAttrs;r=aa(r,a,t)}return aa(r,n.attrs,t)}function of(e,n,t,r,o){let i=null,a=t.directiveEnd,s=t.directiveStylingLast;for(s===-1?s=t.directiveStart:s++;s<a&&(i=n[s],r=aa(r,i.hostAttrs,o),i!==e);)s++;return e!==null&&(t.directiveStylingLast=s),r}function aa(e,n,t){let r=t?1:2,o=-1;if(n!==null)for(let i=0;i<n.length;i++){let a=n[i];typeof a=="number"?o=a:o===r&&(Array.isArray(e)||(e=e===void 0?[]:["",e]),Zs(e,a,t?!0:n[++i]))}return e===void 0?null:e}function JD(e,n,t){if(t==null||t==="")return et;let r=[],o=Qr(t);if(Array.isArray(o))for(let i=0;i<o.length;i++)e(r,o[i],!0);else if(o instanceof Set)for(let i of o)e(r,i,!0);else if(typeof o=="object")for(let i in o)Object.hasOwn(o,i)&&e(r,i,o[i]);else typeof o=="string"&&n(r,o);return r}function eI(e,n,t){let r=String(n);r!==""&&!r.includes(" ")&&Zs(e,r,t)}function tI(e,n,t,r,o,i,a,s){o===ut&&(o=et);let c=0,l=0,d=0<o.length?o[0]:null,u=0<i.length?i[0]:null;for(;d!==null||u!==null;){let g=c<o.length?o[c+1]:void 0,h=l<i.length?i[l+1]:void 0,_=null,D;d===u?(c+=2,l+=2,g!==h&&(_=u,D=h)):u===null||d!==null&&d<u?(c+=2,_=d):(l+=2,_=u,D=h),_!==null&&bv(e,n,t,r,_,D,a,s),d=c<o.length?o[c]:null,u=l<i.length?i[l]:null}}function bv(e,n,t,r,o,i,a,s){if(!(n.type&3))return;let c=e.data,l=c[s+1],d=VD(l)?gb(c,n,t,o,Wo(l),a):void 0;if(!Fc(d)){Fc(i)||PD(l)&&(i=gb(c,null,t,o,s,a));let u=Eu(Ut(),t);jw(r,a,u,o,i)}}function gb(e,n,t,r,o,i){let a=n===null,s;for(;o>0;){let c=e[o],l=Array.isArray(c),d=l?c[1]:c,u=d===null,g=t[o+1];g===ut&&(g=u?et:void 0);let h=u?Ys(g,r):d===r?g:void 0;if(l&&!Fc(h)&&(h=Ys(c,r)),Fc(h)&&(s=h,a))return s;let _=e[o+1];o=a?Jr(_):Wo(_)}if(n!==null){let c=i?n.residualClasses:n.residualStyles;c!=null&&(s=Ys(c,r))}return s}function Fc(e){return e!==void 0}function nI(e,n){return e==null||e===""||(typeof n=="string"?e=Qr(e)+n:typeof e=="object"&&(e=Li(Qr(e)))),e}function yv(e,n){return(e.flags&(n?8:16))!==0}function b(e,n=""){let t=z(),r=Ue(),o=e+Ve,i=r.firstCreatePass?Zo(r,o,1,n,null):r.data[o],a=rI(r,t,i,n);t[o]=a,cc()&&Yf(r,t,a,i),Po(i,!1)}var rI=(e,n,t,r)=>(lc(!0),tw(n[De],r));function oI(e,n,t,r=""){return wn(e,Gr(),t)?n+bn(t)+r:ut}function iI(e,n,t,r,o,i=""){let a=ju(),s=_f(e,a,t,o);return Ki(2),s?n+bn(t)+r+bn(o)+i:ut}function aI(e,n,t,r,o,i,a,s,c,l=""){let d=ju(),u=DE(e,d,t,o,a,c);return Ki(4),u?n+bn(t)+r+bn(o)+i+bn(a)+s+bn(c)+l:ut}function le(e){return ot("",e),le}function ot(e,n,t){let r=z(),o=oI(r,e,n,t);return o!==ut&&pm(r,Ut(),o),ot}function oo(e,n,t,r,o){let i=z(),a=iI(i,e,n,t,r,o);return a!==ut&&pm(i,Ut(),a),oo}function Kc(e,n,t,r,o,i,a,s,c){let l=z(),d=aI(l,e,n,t,r,o,i,a,s,c);return d!==ut&&pm(l,Ut(),d),Kc}function pm(e,n,t){let r=Eu(n,e);nw(e[De],r,t)}function bb(e,n,t){let r=Ue();r.firstCreatePass&&vv(n,r.data,r.blueprint,vn(e),t)}function vv(e,n,t,r,o){if(e=qe(e),Array.isArray(e))for(let i=0;i<e.length;i++)vv(e[i],n,t,r,o);else{let i=Ue(),a=z(),s=rt(),c=Or(e)?e:qe(e.provide),l=_u(e),d=s.providerIndexes&1048575,u=s.directiveStart,g=s.providerIndexes>>20;if(Or(e)||!e.multi){let h=new Yr(l,o,Ie,null),_=sf(c,n,o?d:d+g,u);_===-1?(df(Cc(s,a),i,c),af(i,e,n.length),n.push(c),s.directiveStart++,s.directiveEnd++,o&&(s.providerIndexes+=1048576),t.push(h),a.push(h)):(t[_]=h,a[_]=h)}else{let h=sf(c,n,d+g,u),_=sf(c,n,d,d+g),D=h>=0&&t[h],A=_>=0&&t[_];if(o&&!A||!o&&!D){df(Cc(s,a),i,c);let j=lI(o?cI:sI,t.length,o,r,l,e);!o&&A&&(t[_].providerFactory=j),af(i,e,n.length,0),n.push(c),s.directiveStart++,s.directiveEnd++,o&&(s.providerIndexes+=1048576),t.push(j),a.push(j)}else{let j=_v(t[o?_:h],l,!o&&r);af(i,e,h>-1?h:_,j)}!o&&r&&A&&t[_].componentProviders++}}}function af(e,n,t,r){let o=Or(n),i=sg(n);if(o||i){let c=(i?qe(n.useClass):n).prototype.ngOnDestroy;if(c){let l=e.destroyHooks||(e.destroyHooks=[]);if(!o&&n.multi){let d=l.indexOf(t);d===-1?l.push(t,[r,c]):l[d+1].push(r,c)}else l.push(t,c)}}}function _v(e,n,t){return t&&e.componentProviders++,e.multi.push(n)-1}function sf(e,n,t,r){for(let o=t;o<r;o++)if(n[o]===e)return o;return-1}function sI(e,n,t,r,o){return Lf(this.multi,[])}function cI(e,n,t,r,o){let i=this.multi,a;if(this.providerFactory){let s=this.providerFactory.componentProviders,c=ta(r,r[O],this.providerFactory.index,o);a=c.slice(0,s),Lf(i,a);for(let l=s;l<c.length;l++)a.push(c[l])}else a=[],Lf(i,a);return a}function Lf(e,n){for(let t=0;t<e.length;t++){let r=e[t];n.push(r())}return n}function lI(e,n,t,r,o,i){let a=new Yr(e,t,Ie,null);return a.multi=[],a.index=n,a.componentProviders=0,_v(a,o,r&&!t),a}function ft(e,n){return t=>{t.providersResolver=(r,o)=>bb(r,o?o(e):e,!1),n&&(t.viewProvidersResolver=(r,o)=>bb(r,o?o(n):n,!0))}}function hm(e,n){return Hc(e,n)}var xv=(()=>{class e{applicationErrorHandler=f(Ht);appRef=f(Wn);taskService=f(lr);ngZone=f(Y);zonelessEnabled=f(Xi);tracing=f($n,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new Oe;innerNgZone=this.zoneIsDefined?this.ngZone._inner??null:null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(f(Ku,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let t=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(t);return}this.switchToMicrotaskScheduler(),this.taskService.remove(t)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let t=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})})}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:case 2:{this.appRef.dirtyFlags|=2;break}case 3:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let r=this.useMicrotaskScheduler?Og:$u;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>r(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>r(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.innerNgZone!==null&&this.isInNgZone())}isInNgZone(){let t=Zone.current;for(;t;){if(t===this.innerNgZone)return!0;t=t.parent}return!1}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(r){this.applicationErrorHandler(r)}finally{this.taskService.remove(t),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})();function Cv(){return[{provide:jn,useExisting:xv},{provide:Y,useClass:Pi},{provide:Xi,useValue:!0}]}var gm=(()=>{class e{compileModuleSync(t){return new Ac(t)}compileModuleAsync(t){return Promise.resolve(this.compileModuleSync(t))}clearCache(){}clearCacheFor(t){}getModuleId(t){}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})();function dI(){return typeof $localize<"u"&&$localize.locale||fa}var Zc=new C("",{factory:()=>f(Zc,{optional:!0,skipSelf:!0})||dI()});function $e(e,n){return Ii(e,n?.equal)}function te(e){return hh(e)}var uI=e=>e;function Yc(e,n){if(typeof e=="function"){let t=Ed(e,uI,n?.equal);return Sv(t,void 0,n?.set)}else{let t=Ed(e.source,e.computation,e.equal);return Sv(t,void 0,e.set)}}function Sv(e,n,t){let r=e[Be],o=e;if(t!==void 0){let i=a=>Dd(r,a);o.set=a=>t(a,i),o.update=a=>t(a(te(e)),i)}else o.set=i=>Dd(r,i),o.update=i=>ph(r,i);return o.asReadonly=dc.bind(e),o}function kv(e,n){let t=Object.create(Cb);t.value=e,t.transformFn=n?.transform;function r(){if(An(t),t.value===Vf){let o=null;throw new I(-950,o)}return t.value}return r[Be]=t,r}var pr=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>ca(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function Cm(e){return TI(e)?e.default:e}function TI(e){return e&&typeof e=="object"&&"default"in e}function wv(e,n){return kv(e,n)}function AI(e){return kv(Vf,e)}var En=(wv.required=AI,wv);function Ev(e,n){return sm(n)}function RI(e,n){return cm(n)}var pa=(Ev.required=RI,Ev);function Dv(e,n){return sm(n)}function OI(e,n){return cm(n)}var Mv=(Dv.required=OI,Dv);var FI=1e4;var T$=FI-1e3;var mt=(()=>{class e{static __NG_ELEMENT_ID__=PI}return e})();function PI(e){return LI(rt(),z(),(e&16)===16)}function LI(e,n,t){if(Hn(e)&&!t){let r=jt(e.index,n);return new dr(r,r)}else if(e.type&175){let r=n[nt];return new dr(r,n)}return null}var ym=new C(""),VI=new C("");function ma(e){return!e.moduleRef}function jI(e){let n=ma(e)?e.r3Injector:e.moduleRef.injector,t=n.get(Y);return t.run(()=>{ma(e)?e.r3Injector.resolveInjectorInitializers():e.moduleRef.resolveInjectorInitializers();let r=n.get(Ht),o;if(t.runOutsideAngular(()=>{o=t.onError.subscribe({next:r})}),ma(e)){let i=()=>n.destroy(),a=e.platformInjector.get(ym);a.add(i),n.onDestroy(()=>{o.unsubscribe(),a.delete(i)})}else{let i=()=>e.moduleRef.destroy(),a=e.platformInjector.get(ym);a.add(i),e.moduleRef.onDestroy(()=>{ea(e.allPlatformModules,e.moduleRef),o.unsubscribe(),a.delete(i)})}return UI(r,t,()=>{let i=n.get(lr),a=i.add(),s=n.get(dm);return s.runInitializers(),s.donePromise.then(()=>{let c=n.get(Zc,fa);if(fv(c||fa),!n.get(VI,!0))return ma(e)?n.get(Wn):(e.allPlatformModules.push(e.moduleRef),e.moduleRef);if(ma(e)){let d=n.get(Wn);return e.rootComponent!==void 0&&d.bootstrap(e.rootComponent),d}else return BI?.(e.moduleRef,e.allPlatformModules),e.moduleRef}).finally(()=>{i.remove(a)})})})}var BI;function UI(e,n,t){try{let r=t();return Gn(r)?r.catch(o=>{throw n.runOutsideAngular(()=>e(o)),o}):r}catch(r){throw n.runOutsideAngular(()=>e(r)),r}}var Qc=null;function HI(e=[],n){return xe.create({name:n,providers:[{provide:zi,useValue:"platform"},{provide:ym,useValue:new Set([()=>Qc=null])},...e]})}function zI(e=[]){if(Qc)return Qc;let n=HI(e);return Qc=n,dv(),$I(n),n}function $I(e){let n=e.get(uc,null);Ke(e,()=>{n?.forEach(t=>t())})}function Nv(e){let{rootComponent:n,appProviders:t,platformProviders:r,platformRef:o}=e;he(ae.BootstrapApplicationStart);try{let i=o?.injector??zI(r),a=[Cv(),jg,...t||[]],s=new oa({providers:a,parent:i,debugName:"",runEnvironmentInitializers:!1});return jI({r3Injector:s.injector,platformInjector:i,rootComponent:n})}catch(i){return Promise.reject(i)}finally{he(ae.BootstrapApplicationEnd)}}function ie(e){return typeof e=="boolean"?e:e!=null&&e!=="false"}function Xc(e,n=NaN){return!isNaN(parseFloat(e))&&!isNaN(Number(e))?Number(e):n}var bm=Symbol("NOT_SET"),Tv=new Set,GI=T(v({},vo),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:bm,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(e){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==bm&&!bo(this))return this.signal;try{for(let o of this.cleanup??Tv)o()}finally{this.cleanup?.clear()}let n=[];e!==void 0&&n.push(e),n.push(this.registerCleanupFn);let t=Rn(this),r;try{r=this.userFn.apply(null,n)}finally{Qn(this,t)}return(this.value===bm||!this.equal(this.value,r))&&(this.value=r,this.version++),this.signal}}),vm=class extends Ec{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,t,r,o,i,a=null,s){super(n,[void 0,void 0,void 0,void 0],r,!1,i.get(tt),a),this.scheduler=o;for(let c of Kf){let l=t[c];if(l===void 0)continue;let d=Object.create(GI);d.sequence=this,d.phase=c,d.userFn=l,d.dirty=!0,d.signal=()=>(An(d),d.value),d.signal[Be]=d,d.registerCleanupFn=u=>(d.cleanup??=new Set).add(u),this.nodes[c]=d,this.hooks[c]=u=>d.phaseFn(u)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let t of n.cleanup??Tv)t()}finally{Xn(n)}}};function Sm(e,n){let t=n?.injector??f(xe),r=t.get(jn),o=t.get(qf),i=t.get($n,null,{optional:!0});o.impl??=t.get(dy);let a=e;typeof a=="function"&&(a={mixedReadWrite:e});let s=t.get(Qi,null,{optional:!0}),c=new vm(o.impl,[a.earlyRead,a.write,a.mixedReadWrite,a.read],s?.view,r,t,i?.snapshot(null),void 0);return o.impl.register(c),c}function Av(e,n){let t=rr(e),r=n.elementInjector||To(),i=new Go(t).create(r,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings);return n.onError&&(i.hostView._lView[Oo]=n.onError),i}function Rv(){return!1}var Ov=null;function Gt(){return Ov}function wm(e){Ov??=e}var ha=class{},io=(()=>{class e{historyGo(t){throw new Error("")}static \u0275fac=function(r){return new(r||e)};static \u0275prov=G({token:e,factory:()=>f(Fv),providedIn:"platform"})}return e})();var Fv=(()=>{class e extends io{_location;_history;_doc=f(ye);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return Gt().getBaseHref(this._doc)}onPopState(t){let r=Gt().getGlobalEventTarget(this._doc,"window");return r.addEventListener("popstate",t,!1),()=>r.removeEventListener("popstate",t)}onHashChange(t){let r=Gt().getGlobalEventTarget(this._doc,"window");return r.addEventListener("hashchange",t,!1),()=>r.removeEventListener("hashchange",t)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(t){this._location.pathname=t}pushState(t,r,o){this._history.pushState(t,r,o)}replaceState(t,r,o){this._history.replaceState(t,r,o)}forward(){this._history.forward()}back(){this._history.back()}historyGo(t=0){this._history.go(t)}getState(){return this._history.state}static \u0275fac=function(r){return new(r||e)};static \u0275prov=G({token:e,factory:()=>new e,providedIn:"platform"})}return e})();function Jc(e,n){return e?n?e.endsWith("/")?n.startsWith("/")?e+n.slice(1):e+n:n.startsWith("/")?e+n:`${e}/${n}`:e:n}function Pv(e){let n=e.search(/#|\?|$/);return e[n-1]==="/"?e.slice(0,n-1)+e.slice(n):e}function sn(e){return e&&e[0]!=="?"?`?${e}`:e}var Dn=(()=>{class e{historyGo(t){throw new Error("")}static \u0275fac=function(r){return new(r||e)};static \u0275prov=G({token:e,factory:()=>f(Vv),providedIn:"root"})}return e})(),Em=new C(""),Vv=(()=>{class e extends Dn{_platformLocation;_baseHref;_removeListenerFns=[];constructor(t,r){super(),this._platformLocation=t,this._baseHref=r??this._platformLocation.getBaseHrefFromDOM()??f(ye).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(t){this._removeListenerFns.push(this._platformLocation.onPopState(t),this._platformLocation.onHashChange(t))}getBaseHref(){return this._baseHref}prepareExternalUrl(t){return Jc(this._baseHref,t)}path(t=!1){let r=this._platformLocation.pathname+sn(this._platformLocation.search),o=this._platformLocation.hash;return o&&t?`${r}${o}`:r}pushState(t,r,o,i){let a=this.prepareExternalUrl(o+sn(i));this._platformLocation.pushState(t,r,a)}replaceState(t,r,o,i){let a=this.prepareExternalUrl(o+sn(i));this._platformLocation.replaceState(t,r,a)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(t=0){this._platformLocation.historyGo?.(t)}static \u0275fac=function(r){return new(r||e)(H(io),H(Em,8))};static \u0275prov=G({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var ao=(()=>{class e{_subject=new q;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(t){this._locationStrategy=t;let r=this._locationStrategy.getBaseHref();this._basePath=KI(Pv(Lv(r))),this._locationStrategy.onPopState(o=>{let i={url:this.path(!0),pop:!0,state:o.state,type:o.type};o.hasUAVisualTransition&&(i.hasUAVisualTransition=!0),this._subject.next(i)})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(t=!1){return this.normalize(this._locationStrategy.path(t))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(t,r=""){return this.path()==this.normalize(t+sn(r))}normalize(t){return e.stripTrailingSlash(qI(this._basePath,Lv(t)))}prepareExternalUrl(t){return t&&t[0]!=="/"&&(t="/"+t),this._locationStrategy.prepareExternalUrl(t)}go(t,r="",o=null){this._locationStrategy.pushState(o,"",t,r),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+sn(r)),o)}replaceState(t,r="",o=null){this._locationStrategy.replaceState(o,"",t,r),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+sn(r)),o)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(t=0){this._locationStrategy.historyGo?.(t)}onUrlChange(t){return this._urlChangeListeners.push(t),this._urlChangeSubscription??=this.subscribe(r=>{this._notifyUrlChangeListeners(r.url,r.state)}),()=>{let r=this._urlChangeListeners.indexOf(t);this._urlChangeListeners.splice(r,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(t="",r){this._urlChangeListeners.forEach(o=>o(t,r))}subscribe(t,r,o){return this._subject.subscribe({next:t,error:r??void 0,complete:o??void 0})}static normalizeQueryParams=sn;static joinWithSlash=Jc;static stripTrailingSlash=Pv;static \u0275fac=function(r){return new(r||e)(H(Dn))};static \u0275prov=G({token:e,factory:()=>WI(),providedIn:"root"})}return e})();function WI(){return new ao(H(Dn))}function qI(e,n){if(!e||!n.startsWith(e))return n;let t=n.substring(e.length);return t===""||["/",";","?","#"].includes(t[0])?t:n}function Lv(e){return e.replace(/\/index\.html$/,"")}function KI(e){if(new RegExp("^(https?:)?//").test(e)){let[,t]=e.split(/\/\/[^\/]+/);return t}return e}var Dm=(()=>{class e extends Dn{_platformLocation;_baseHref="";_removeListenerFns=[];constructor(t,r){super(),this._platformLocation=t,r!=null&&(this._baseHref=r)}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(t){this._removeListenerFns.push(this._platformLocation.onPopState(t),this._platformLocation.onHashChange(t))}getBaseHref(){return this._baseHref}path(t=!1){let r=this._platformLocation.hash??"#";return r.length>0?r.substring(1):r}prepareExternalUrl(t){let r=Jc(this._baseHref,t);return r.length>0?"#"+r:r}pushState(t,r,o,i){let a=this.prepareExternalUrl(o+sn(i))||this._platformLocation.pathname;this._platformLocation.pushState(t,r,a)}replaceState(t,r,o,i){let a=this.prepareExternalUrl(o+sn(i))||this._platformLocation.pathname;this._platformLocation.replaceState(t,r,a)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(t=0){this._platformLocation.historyGo?.(t)}static \u0275fac=function(r){return new(r||e)(H(io),H(Em,8))};static \u0275prov=G({token:e,factory:e.\u0275fac})}return e})();var Im=(()=>{class e{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=f(xe);constructor(t){this._viewContainerRef=t}ngOnChanges(t){if(this._shouldRecreateView(t)){let r=this._viewContainerRef;if(this._viewRef&&r.remove(r.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let o=this._createContextForwardProxy();this._viewRef=r.createEmbeddedView(this.ngTemplateOutlet,o,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(t){return!!t.ngTemplateOutlet||!!t.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(t,r,o)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,r,o):!1,get:(t,r,o)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,r,o)}})}static \u0275fac=function(r){return new(r||e)(Ie(ur))};static \u0275dir=re({type:e,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[yt]})}return e})();function km(e,n){n=encodeURIComponent(n);for(let t of e.split(";")){let r=t.indexOf("="),[o,i]=r==-1?[t,""]:[t.slice(0,r),t.slice(r+1)];if(o.trim()!==n)continue;let a=i;try{a=decodeURIComponent(i)}catch{}return a.length>1&&a[0]==='"'&&a[a.length-1]==='"'&&(a=a.slice(1,-1)),a}return null}var Mm="browser";function jv(e){return e===Mm}var ga=class{_doc;constructor(n){this._doc=n}manager},el=(()=>{class e extends ga{constructor(t){super(t)}supports(t){return!0}addEventListener(t,r,o,i){return t.addEventListener(r,o,i),()=>this.removeEventListener(t,r,o,i)}removeEventListener(t,r,o,i){return t.removeEventListener(r,o,i)}static \u0275fac=function(r){return new(r||e)(H(ye))};static \u0275prov=G({token:e,factory:e.\u0275fac})}return e})(),rl=new C(""),Rm=(()=>{class e{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,r){this._zone=r,t.forEach(a=>{a.manager=this});let o=t.filter(a=>!(a instanceof el));this._plugins=o.slice().reverse();let i=t.find(a=>a instanceof el);i&&this._plugins.push(i)}addEventListener(t,r,o,i){return this._findPluginFor(r).addEventListener(t,r,o,i)}getZone(){return this._zone}_findPluginFor(t){let r=this._eventNameToPlugin.get(t);if(r)return r;if(r=this._plugins.find(i=>i.supports(t)),!r)throw new I(-5101,!1);return this._eventNameToPlugin.set(t,r),r}static \u0275fac=function(r){return new(r||e)(H(rl),H(Y))};static \u0275prov=G({token:e,factory:e.\u0275fac})}return e})(),Nm="ng-app-id";function Bv(e){for(let n of e)n.remove()}function Uv(e,n){let t=n.createElement("style");return t.textContent=e,t}function JI(e,n,t,r){let o=e.head?.querySelectorAll(`style[${Nm}="${n}"],link[${Nm}="${n}"]`);if(!o||o.length===0)return!1;for(let i of o)i.removeAttribute(Nm),i instanceof HTMLLinkElement?r.set(i.href.slice(i.href.lastIndexOf("/")+1),{usage:0,elements:[i]}):i.textContent&&t.set(i.textContent,{usage:0,elements:[i]});return!0}function Am(e,n){let t=n.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",e),t}var Om=(()=>{class e{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(t,r,o,i={}){this.doc=t,this.appId=r,this.nonce=o,JI(t,r,this.inline,this.external)&&this.hosts.add(t.head)}addStyles(t,r){for(let o of t)this.addUsage(o,this.inline,Uv);r?.forEach(o=>this.addUsage(o,this.external,Am))}removeStyles(t,r){for(let o of t)this.removeUsage(o,this.inline);r?.forEach(o=>this.removeUsage(o,this.external))}addUsage(t,r,o){let i=r.get(t);i?i.usage++:r.set(t,{usage:1,elements:[...this.hosts].map(a=>this.addElement(a,o(t,this.doc)))})}removeUsage(t,r){let o=r.get(t);o&&(o.usage--,o.usage<=0&&(Bv(o.elements),r.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])Bv(t);this.hosts.clear()}addHost(t){if(!this.hosts.has(t)){this.hosts.add(t);for(let[r,{elements:o}]of this.inline)o.push(this.addElement(t,Uv(r,this.doc)));for(let[r,{elements:o}]of this.external)o.push(this.addElement(t,Am(r,this.doc)))}}removeHost(t){this.hosts.delete(t);for(let r of[...this.inline.values(),...this.external.values()]){let o=[];for(let i of r.elements)i.parentNode===t?i.remove():o.push(i);r.elements=o}}addElement(t,r){return this.nonce&&r.setAttribute("nonce",this.nonce),t.appendChild(r)}static \u0275fac=function(r){return new(r||e)(H(ye),H(Wr),H(Kr,8),H(qr))};static \u0275prov=G({token:e,factory:e.\u0275fac})}return e})(),Tm={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Fm=/%COMP%/g;var zv="%COMP%",ek=`_nghost-${zv}`,tk=`_ngcontent-${zv}`,nk=!0,rk=new C("",{factory:()=>nk}),ok=new C("");function ik(e){return tk.replace(Fm,e)}function ak(e){return ek.replace(Fm,e)}function $v(e,n){return n.map(t=>t.replace(Fm,e))}var Pm=(()=>{class e{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;cssVarNamespace;constructor(t,r,o,i,a,s,c=null,l=null,d=null){this.eventManager=t,this.sharedStylesHost=r,this.appId=o,this.removeStylesOnCompDestroy=i,this.doc=a,this.ngZone=s,this.nonce=c,this.tracingService=l,this.cssVarNamespace=d??"",this.defaultRenderer=new ba(t,a,s,this.tracingService,this.cssVarNamespace)}createRenderer(t,r){if(!t||!r)return this.defaultRenderer;let o=this.getOrCreateRenderer(t,r);return o instanceof nl?o.applyToHost(t):o instanceof ya&&o.applyStyles(),o}getOrCreateRenderer(t,r){let o=this.rendererByCompId,i=o.get(r.id);if(!i){let a=this.doc,s=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,u=this.tracingService;switch(r.encapsulation){case an.Emulated:i=new nl(c,l,r,this.appId,d,a,s,u,this.cssVarNamespace);break;case an.ShadowDom:return new tl(c,t,r,a,s,this.nonce,u,this.cssVarNamespace,l);case an.ExperimentalIsolatedShadowDom:return new tl(c,t,r,a,s,this.nonce,u,this.cssVarNamespace);default:i=new ya(c,l,r,d,a,s,u,this.cssVarNamespace);break}o.set(r.id,i)}return i}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(r){return new(r||e)(H(Rm),H(to),H(Wr),H(rk),H(ye),H(Y),H(Kr),H($n,8),H(ok,8))};static \u0275prov=G({token:e,factory:e.\u0275fac})}return e})(),ba=class{eventManager;doc;ngZone;tracingService;cssVarNamespace;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,t,r,o,i=""){this.eventManager=n,this.doc=t,this.ngZone=r,this.tracingService=o,this.cssVarNamespace=i}destroy(){}destroyNode=null;createElement(n,t){return t?this.doc.createElementNS(Tm[t]||t,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,t){(Hv(n)?n.content:n).appendChild(t)}insertBefore(n,t,r){if(n){let o=Hv(n)?n.content:n;if(r!=null&&r.parentNode!==o)throw new I(-5106,!1);o.insertBefore(t,r)}}removeChild(n,t){t.remove()}selectRootElement(n,t){let r=typeof n=="string"?this.doc.querySelector(n):n;if(!r)throw new I(-5104,!1);return t||(r.textContent=""),r}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,t,r,o){if(o){t=o+":"+t;let i=Tm[o];i?n.setAttributeNS(i,t,r):n.setAttribute(t,r)}else n.setAttribute(t,r)}removeAttribute(n,t,r){if(r){let o=Tm[r];o?n.removeAttributeNS(o,t):n.removeAttribute(`${r}:${t}`)}else n.removeAttribute(t)}addClass(n,t){n.classList.add(t)}removeClass(n,t){n.classList.remove(t)}setStyle(n,t,r,o){let i=t.startsWith("--");i&&(t=t.replace("%NS%",this.cssVarNamespace)),i||o&(Sn.DashCase|Sn.Important)?n.style.setProperty(t,r,o&Sn.Important?"important":""):n.style[t]=r}removeStyle(n,t,r){let o=t.startsWith("--");o&&(t=t.replace("%NS%",this.cssVarNamespace)),o||r&Sn.DashCase?n.style.removeProperty(t):n.style[t]=""}setProperty(n,t,r){n!=null&&(n[t]=r)}setValue(n,t){n.nodeValue=t}listen(n,t,r,o){if(typeof n=="string"&&(n=Gt().getGlobalEventTarget(this.doc,n),!n))throw new I(-5102,!1);let i=this.decoratePreventDefault(r);return this.tracingService?.wrapEventListener&&(i=this.tracingService.wrapEventListener(n,t,i)),this.eventManager.addEventListener(n,t,i,o)}decoratePreventDefault(n){return t=>{if(t==="__ngUnwrap__")return n;n(t)===!1&&t.preventDefault()}}};function Hv(e){return e.tagName==="TEMPLATE"&&e.content!==void 0}var tl=class extends ba{hostEl;sharedStylesHost;shadowRoot;constructor(n,t,r,o,i,a,s,c,l){super(n,o,i,s,c),this.hostEl=t,this.sharedStylesHost=l,this.shadowRoot=t.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let d=r.styles;d=$v(r.id,d).map(g=>g.replace(/%NS%/g,c));for(let g of d){let h=document.createElement("style");a&&h.setAttribute("nonce",a),h.textContent=g,this.shadowRoot.appendChild(h)}let u=r.getExternalStyles?.();if(u)for(let g of u){let h=Am(g,o);a&&h.setAttribute("nonce",a),this.shadowRoot.appendChild(h)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,t){return super.appendChild(this.nodeOrShadowRoot(n),t)}insertBefore(n,t,r){return super.insertBefore(this.nodeOrShadowRoot(n),t,r)}removeChild(n,t){return super.removeChild(null,t)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},ya=class extends ba{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,t,r,o,i,a,s,c,l){super(n,i,a,s,c),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=o;let d=r.styles,u=l?$v(l,d):d;this.styles=u.map(g=>g.replace(/%NS%/g,c)),this.styleUrls=r.getExternalStyles?.(l)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Wf.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},nl=class extends ya{contentAttr;hostAttr;constructor(n,t,r,o,i,a,s,c,l){let d=o+"-"+r.id;super(n,t,r,i,a,s,c,l,d),this.contentAttr=ik(d),this.hostAttr=ak(d)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,t){let r=super.createElement(n,t);return super.setAttribute(r,this.contentAttr,""),r}};var ol=class e extends ha{supportsDOMEvents=!0;static makeCurrent(){wm(new e)}onAndCancel(n,t,r,o){return n.addEventListener(t,r,o),()=>{n.removeEventListener(t,r,o)}}dispatchEvent(n,t){n.dispatchEvent(t)}remove(n){n.remove()}createElement(n,t){return t=t||this.getDefaultDocument(),t.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,t){return t==="window"?window:t==="document"?n:t==="body"?n.body:null}getBaseHref(n){let t=sk();return t==null?null:ck(t)}resetBaseElement(){va=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return km(document.cookie,n)}},va=null;function sk(){return va=va||document.head.querySelector("base"),va?va.getAttribute("href"):null}function ck(e){return new URL(e,document.baseURI).pathname}var Gv=["alt","control","meta","shift"],lk={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},dk={alt:e=>e.altKey,control:e=>e.ctrlKey,meta:e=>e.metaKey,shift:e=>e.shiftKey},Wv=(()=>{class e extends ga{constructor(t){super(t)}supports(t){return e.parseEventName(t)!=null}addEventListener(t,r,o,i){let a=e.parseEventName(r),s=e.eventCallback(a.fullKey,o,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>Gt().onAndCancel(t,a.domEventName,s,i))}static parseEventName(t){let r=t.toLowerCase().split("."),o=r.shift();if(r.length===0||!(o==="keydown"||o==="keyup"))return null;let i=e._normalizeKey(r.pop()),a="",s=r.indexOf("code");if(s>-1&&(r.splice(s,1),a="code."),Gv.forEach(l=>{let d=r.indexOf(l);d>-1&&(r.splice(d,1),a+=l+".")}),a+=i,r.length!=0||i.length===0)return null;let c={};return c.domEventName=o,c.fullKey=a,c}static matchEventFullKeyCode(t,r){let o=lk[t.key]||t.key,i="";return r.indexOf("code.")>-1&&(o=t.code,i="code."),o==null||!o?!1:(o=o.toLowerCase(),o===" "?o="space":o==="."&&(o="dot"),Gv.forEach(a=>{if(a!==o){let s=dk[a];s(t)&&(i+=a+".")}}),i+=o,i===r)}static eventCallback(t,r,o){return i=>{e.matchEventFullKeyCode(i,t)&&o.runGuarded(()=>r(i))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(r){return new(r||e)(H(ye))};static \u0275prov=G({token:e,factory:e.\u0275fac})}return e})();async function Lm(e,n,t){let r=v({rootComponent:e},uk(n,t));return Nv(r)}function uk(e,n){return{platformRef:n?.platformRef,appProviders:[...gk,...e?.providers??[]],platformProviders:hk}}function fk(){ol.makeCurrent()}function mk(){return new en}function pk(){return Uf(document),document}var hk=[{provide:qr,useValue:Mm},{provide:uc,useValue:fk,multi:!0},{provide:ye,useFactory:pk}];var gk=[{provide:zi,useValue:"root"},{provide:en,useFactory:mk},{provide:rl,useClass:el,multi:!0},{provide:rl,useClass:Wv,multi:!0},Pm,{provide:to,useClass:Om},{provide:Om,useExisting:to},Rm,{provide:It,useExisting:Pm},[]];var qv=(()=>{class e{_doc;constructor(t){this._doc=t}getTitle(){return this._doc.title}setTitle(t){this._doc.title=t||""}static \u0275fac=function(r){return new(r||e)(H(ye))};static \u0275prov=G({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var U="primary",Ra=Symbol("RouteTitle"),zm=class{params;constructor(n){this.params=n||{}}has(n){return Object.hasOwn(this.params,n)}get(n){if(this.has(n)){let t=this.params[n];return Array.isArray(t)?t[0]:t}return null}getAll(n){if(this.has(n)){let t=this.params[n];return Array.isArray(t)?t:[t]}return[]}get keys(){return Object.keys(this.params)}};function co(e){return new zm(e)}function Vm(e,n,t){for(let r=0;r<e.length;r++){let o=e[r],i=n[r];if(o[0]===":")t[o.substring(1)]=i;else if(o!==i.path)return!1}return!0}function t_(e,n,t){let r=t.path.split("/"),o=r.indexOf("**");if(o===-1){if(r.length>e.length||t.pathMatch==="full"&&(n.hasChildren()||r.length<e.length))return null;let c={},l=e.slice(0,r.length);return Vm(r,l,c)?{consumed:l,posParams:c}:null}if(o!==r.lastIndexOf("**"))return null;let i=r.slice(0,o),a=r.slice(o+1);if(i.length+a.length>e.length||t.pathMatch==="full"&&n.hasChildren()&&t.path!=="**")return null;let s={};return!Vm(i,e.slice(0,i.length),s)||!Vm(a,e.slice(e.length-a.length),s)?null:{consumed:e,posParams:s}}function dl(e){return new Promise((n,t)=>{e.pipe(Fn()).subscribe({next:r=>n(r),error:r=>t(r)})})}function vk(e,n){if(e.length!==n.length)return!1;for(let t=0;t<e.length;++t)if(!kn(e[t],n[t]))return!1;return!0}function kn(e,n){let t=e?$m(e):void 0,r=n?$m(n):void 0;if(!t||!r||t.length!=r.length)return!1;let o;for(let i=0;i<t.length;i++)if(o=t[i],!n_(e[o],n[o]))return!1;return!0}function $m(e){return[...Object.keys(e),...Object.getOwnPropertySymbols(e)]}function n_(e,n){if(Array.isArray(e)&&Array.isArray(n)){if(e.length!==n.length)return!1;let t=[...e].sort(),r=[...n].sort();return t.every((o,i)=>r[i]===o)}else return e===n}function _k(e){return e.length>0?e[e.length-1]:null}function Oa(e){return Ns(e)?e:Gn(e)?we(Promise.resolve(e)):X(e)}function r_(e){return Ns(e)?dl(e):Promise.resolve(e)}var xk={exact:a_,subset:s_},o_={exact:Ck,subset:Sk,ignored:()=>!0},i_={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},ul={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function Gm(e,n,t){let r=v(v({},ul),t||{});return xk[r.paths](e.root,n.root,r.matrixParams)&&o_[r.queryParams](e.queryParams,n.queryParams)&&!(r.fragment==="exact"&&e.fragment!==n.fragment)}function Ck(e,n){return kn(e,n)}function a_(e,n,t){if(!so(e.segments,n.segments)||!sl(e.segments,n.segments,t)||e.numberOfChildren!==n.numberOfChildren)return!1;for(let r in n.children)if(!e.children[r]||!a_(e.children[r],n.children[r],t))return!1;return!0}function Sk(e,n){return Object.keys(n).length<=Object.keys(e).length&&Object.keys(n).every(t=>n_(e[t],n[t]))}function s_(e,n,t){return c_(e,n,n.segments,t)}function c_(e,n,t,r){if(e.segments.length>t.length){let o=e.segments.slice(0,t.length);return!(!so(o,t)||n.hasChildren()||!sl(o,t,r))}else if(e.segments.length===t.length){if(!so(e.segments,t)||!sl(e.segments,t,r))return!1;for(let o in n.children)if(!e.children[o]||!s_(e.children[o],n.children[o],r))return!1;return!0}else{let o=t.slice(0,e.segments.length),i=t.slice(e.segments.length);return!so(e.segments,o)||!sl(e.segments,o,r)||!e.children[U]?!1:c_(e.children[U],n,i,r)}}function sl(e,n,t){return n.every((r,o)=>o_[t](e[o].parameters,r.parameters))}var Nt=class{root;queryParams;fragment;_queryParamMap;constructor(n=new me([],{}),t={},r=null){this.root=n,this.queryParams=t,this.fragment=r}get queryParamMap(){return this._queryParamMap??=co(this.queryParams),this._queryParamMap}toString(){return Ik.serialize(this)}},me=class{segments;children;parent=null;constructor(n,t){this.segments=n,this.children=t,Object.values(t).forEach(r=>r.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return cl(this)}},gr=class{path;parameters;_parameterMap;constructor(n,t){this.path=n,this.parameters=t}get parameterMap(){return this._parameterMap??=co(this.parameters),this._parameterMap}toString(){return d_(this)}};function wk(e,n){return so(e,n)&&e.every((t,r)=>kn(t.parameters,n[r].parameters))}function so(e,n){return e.length!==n.length?!1:e.every((t,r)=>t.path===n[r].path)}function Ek(e,n){let t=[];return Object.entries(e.children).forEach(([r,o])=>{r===U&&(t=t.concat(n(o,r)))}),Object.entries(e.children).forEach(([r,o])=>{r!==U&&(t=t.concat(n(o,r)))}),t}var si=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:()=>new br})}return e})(),br=class{parse(n){let t=new qm(n);return new Nt(t.parseRootSegment(),t.parseQueryParams(),t.parseFragment())}serialize(n){let t=`/${Jo(n.root,!0)}`;Dk(t)&&(t="/");let r=Tk(n.queryParams),o=typeof n.fragment=="string"?`#${Mk(n.fragment)}`:"";return`${t}${r}${o}`}},Kv="http://fake";function Dk(e){try{let n=new URL(e,Kv);return n.origin!==Kv||n.pathname.startsWith("//")}catch{return!0}}var Ik=new br;function cl(e){return e.segments.map(n=>d_(n)).join("/")}function Jo(e,n){if(!e.hasChildren())return cl(e);if(n){let t=e.children[U]?Jo(e.children[U],!1):"",r=[];return Object.entries(e.children).forEach(([o,i])=>{o!==U&&r.push(`${o}:${Jo(i,!1)}`)}),r.length>0?`${t}(${r.join("//")})`:t}else{if(Object.keys(e.children).length===1&&e.children[U]!=null)return`${cl(e)}/${Jo(e.children[U],!1)}`;let t=Ek(e,(r,o)=>o===U?[kk(Jo(e.children[U],!1))]:[`${o}:${Jo(r,!1)}`]);return`${cl(e)}/(${t.join("//")})`}}function kk(e){return e.replace(u_,n=>n.replace(/:/g,"%3A"))}function l_(e){return encodeURIComponent(e).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function il(e){return l_(e).replace(/%3B/gi,";")}function Mk(e){return encodeURI(e)}function Wm(e){return l_(e).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function ll(e){return decodeURIComponent(e)}function Zv(e){return ll(e.replace(/\+/g,"%20"))}function d_(e){return`${Wm(e.path)}${Nk(e.parameters)}`}function Nk(e){return Object.entries(e).map(([n,t])=>`;${Wm(n)}=${Wm(t)}`).join("")}function Tk(e){let n=Object.entries(e).map(([t,r])=>Array.isArray(r)?r.map(o=>`${il(t)}=${il(o)}`).join("&"):`${il(t)}=${il(r)}`).filter(t=>t);return n.length?`?${n.join("&")}`:""}var jm=1073741824;function fl(e,n,t){Number(n)>=32&&!Object.hasOwn(e,jm)&&(e[jm]=t,delete e[jm]),e[n]=t}var u_=/^[^\/()?;#]+/;function Bm(e){let n=e.match(u_);return n?n[0]:""}var Ak=/^[^\/()?;=#]+/;function Rk(e){let n=e.match(Ak);return n?n[0]:""}var Ok=/^[^=?&#]+/;function Fk(e){let n=e.match(Ok);return n?n[0]:""}var Pk=/^[^&#]+/;function Lk(e){let n=e.match(Pk);return n?n[0]:""}var qm=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new me([],{}):new me([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new I(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let t=[];for(this.peekStartsWith("(")||t.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),t.push(this.parseSegment());let r={};this.peekStartsWith("/(")&&(this.capture("/"),r=this.parseParens(!0,n));let o={};return this.peekStartsWith("(")&&(o=this.parseParens(!1,n)),(t.length>0||Object.keys(r).length>0)&&(o[U]=new me(t,r)),o}parseSegment(){let n=Bm(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new I(4009,!1);return this.capture(n),new gr(ll(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let t=Rk(this.remaining);if(!t)return;this.capture(t);let r="";if(this.consumeOptional("=")){let o=Bm(this.remaining);o&&(r=o,this.capture(r))}fl(n,ll(t),ll(r))}parseQueryParam(n){let t=Fk(this.remaining);if(!t)return;this.capture(t);let r="";if(this.consumeOptional("=")){let a=Lk(this.remaining);a&&(r=a,this.capture(r))}let o=Zv(t),i=Zv(r);if(Object.hasOwn(n,o)){let a=n[o];Array.isArray(a)||(a=[a],n[o]=a),a.push(i)}else n[o]=i}parseParens(n,t){let r=Object.create(null);for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let o=Bm(this.remaining),i=this.remaining[o.length];if(i!=="/"&&i!==")"&&i!==";")throw new I(4010,!1);let a;o.indexOf(":")>-1?(a=o.slice(0,o.indexOf(":")),this.capture(a),this.capture(":")):n&&(a=U);let s=this.parseChildren(t+1),c=Object.keys(s).length===1&&s[U]?s[U]:new me([],s);fl(r,a??U,c),this.consumeOptional("//")}return r}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new I(4011,!1)}};function f_(e){return e.segments.length>0?new me([],{[U]:e}):e}function m_(e){let n=Object.create(null);for(let[r,o]of Object.entries(e.children)){let i=m_(o);if(r===U&&i.segments.length===0&&i.hasChildren())for(let[a,s]of Object.entries(i.children))fl(n,a,s);else(i.segments.length>0||i.hasChildren())&&fl(n,r,i)}let t=new me(e.segments,n);return Vk(t)}function Vk(e){if(e.numberOfChildren===1&&e.children[U]){let n=e.children[U];return new me(e.segments.concat(n.segments),n.children)}return e}function yr(e){return e instanceof Nt}function p_(e,n,t=null,r=null,o=new br){let i=h_(e);return g_(i,n,t,r,o)}function h_(e){let n;function t(i){let a={};for(let c of i.children){let l=t(c);a[c.outlet]=l}let s=new me(i.url,a);return i===e&&(n=s),s}let r=t(e.root),o=f_(r);return n??o}function g_(e,n,t,r,o){let i=e;for(;i.parent;)i=i.parent;if(n.length===0)return Um(i,i,i,t,r,o);let a=jk(n);if(a.toRoot())return Um(i,i,new me([],{}),t,r,o);let s=Bk(a,i,e),c=s.processChildren?xa(s.segmentGroup,s.index,a.commands):y_(s.segmentGroup,s.index,a.commands);return Um(i,s.segmentGroup,c,t,r,o)}function ml(e){return typeof e=="object"&&e!=null&&!e.outlets&&!e.segmentPath}function Ea(e){return typeof e=="object"&&e!=null&&e.outlets}function Yv(e,n,t){e||="\u0275";let r=new Nt;return r.queryParams={[e]:n},t.parse(t.serialize(r)).queryParams[e]}function Um(e,n,t,r,o,i){let a={};for(let[l,d]of Object.entries(r??{}))d!=null&&(a[l]=Array.isArray(d)?d.map(u=>Yv(l,u,i)):Yv(l,d,i));let s;e===n?s=t:s=b_(e,n,t);let c=f_(m_(s));return new Nt(c,a,o)}function b_(e,n,t){let r=Object.create(null);return Object.entries(e.children).forEach(([o,i])=>{i===n?r[o]=t:r[o]=b_(i,n,t)}),new me(e.segments,r)}var pl=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,t,r){if(this.isAbsolute=n,this.numberOfDoubleDots=t,this.commands=r,n&&r.length>0&&ml(r[0]))throw new I(4003,!1);let o=r.find(Ea);if(o&&o!==_k(r))throw new I(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function jk(e){if(typeof e[0]=="string"&&e.length===1&&e[0]==="/")return new pl(!0,0,e);let n=0,t=!1,r=e.reduce((o,i,a)=>{if(typeof i=="object"&&i!=null){if(i.outlets){let s={};return Object.entries(i.outlets).forEach(([c,l])=>{s[c]=typeof l=="string"?l.split("/"):l}),[...o,{outlets:s}]}if(i.segmentPath)return[...o,i.segmentPath]}return typeof i!="string"?[...o,i]:a===0?(i.split("/").forEach((s,c)=>{c==0&&s==="."||(c==0&&s===""?t=!0:s===".."?n++:s!=""&&o.push(s))}),o):[...o,i]},[]);return new pl(t,n,r)}var ti=class{segmentGroup;processChildren;index;constructor(n,t,r){this.segmentGroup=n,this.processChildren=t,this.index=r}};function Bk(e,n,t){if(e.isAbsolute)return new ti(n,!0,0);if(!t)return new ti(n,!1,NaN);if(t.parent===null)return new ti(t,!0,0);let r=ml(e.commands[0])?0:1,o=t.segments.length-1+r;return Uk(t,o,e.numberOfDoubleDots)}function Uk(e,n,t){let r=e,o=n,i=t;for(;i>o;){if(i-=o,r=r.parent,!r)throw new I(4005,!1);o=r.segments.length}return new ti(r,!1,o-i)}function Hk(e){return Ea(e[0])?e[0].outlets:{[U]:e}}function y_(e,n,t){if(e??=new me([],{}),e.segments.length===0&&e.hasChildren())return xa(e,n,t);let r=zk(e,n,t),o=t.slice(r.commandIndex);if(r.match&&r.pathIndex<e.segments.length){let i=new me(e.segments.slice(0,r.pathIndex),{});return i.children[U]=new me(e.segments.slice(r.pathIndex),e.children),xa(i,0,o)}else return r.match&&o.length===0?new me(e.segments,{}):r.match&&!e.hasChildren()?Km(e,n,t):r.match?xa(e,0,o):Km(e,n,t)}function xa(e,n,t){if(t.length===0)return new me(e.segments,{});{let r=Hk(t),o=Object.create(null);if(Object.keys(r).some(i=>i!==U)&&e.children[U]&&e.numberOfChildren===1&&e.children[U].segments.length===0){let i=xa(e.children[U],n,t);return new me(e.segments,i.children)}return Object.entries(r).forEach(([i,a])=>{typeof a=="string"&&(a=[a]),a!==null&&(o[i]=y_(e.children[i],n,a))}),Object.entries(e.children).forEach(([i,a])=>{r[i]===void 0&&(o[i]=a)}),new me(e.segments,o)}}function zk(e,n,t){let r=0,o=n,i={match:!1,pathIndex:0,commandIndex:0};for(;o<e.segments.length;){if(r>=t.length)return i;let a=e.segments[o],s=t[r];if(Ea(s))break;let c=`${s}`,l=r<t.length-1?t[r+1]:null;if(o>0&&c===void 0)break;if(c&&l&&typeof l=="object"&&l.outlets===void 0){if(!Xv(c,l,a))return i;r+=2}else{if(!Xv(c,{},a))return i;r++}o++}return{match:!0,pathIndex:o,commandIndex:r}}function Km(e,n,t){let r=e.segments.slice(0,n),o=0;for(;o<t.length;){let i=t[o];if(Ea(i)){let c=$k(i.outlets);return new me(r,c)}if(o===0&&ml(t[0])){let c=e.segments[n];r.push(new gr(c.path,Qv(t[0]))),o++;continue}let a=Ea(i)?i.outlets[U]:`${i}`,s=o<t.length-1?t[o+1]:null;a&&s&&ml(s)?(r.push(new gr(a,Qv(s))),o+=2):(r.push(new gr(a,{})),o++)}return new me(r,{})}function $k(e){let n={};return Object.entries(e).forEach(([t,r])=>{typeof r=="string"&&(r=[r]),r!==null&&(n[t]=Km(new me([],{}),0,r))}),n}function Qv(e){let n={};return Object.entries(e).forEach(([t,r])=>n[t]=`${r}`),n}function Xv(e,n,t){return e==t.path&&kn(n,t.parameters)}var hl=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new lo(this.rootInjector)}resetChildren(){this.children=new lo(this.rootInjector)}},lo=(()=>{class e{rootInjector;contexts=new Map;constructor(t){this.rootInjector=t}onChildOutletCreated(t,r){let o=this.getOrCreateContext(t);o.outlet=r,this.contexts.set(t,o)}onChildOutletDestroyed(t){let r=this.getContext(t);r&&(r.outlet=null,r.attachRef=null)}onOutletDeactivated(){let t=this.contexts;return this.contexts=new Map,t}onOutletReAttached(t){this.contexts=t}getOrCreateContext(t){let r=this.getContext(t);return r||(r=new hl(this.rootInjector),this.contexts.set(t,r)),r}getContext(t){return this.contexts.get(t)||null}static \u0275fac=function(r){return new(r||e)(H(Pe))};static \u0275prov=G({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),gl=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let t=this.pathFromRoot(n);return t.length>1?t[t.length-2]:null}children(n){let t=Zm(n,this._root);return t?t.children.map(r=>r.value):[]}firstChild(n){let t=Zm(n,this._root);return t&&t.children.length>0?t.children[0].value:null}siblings(n){let t=Ym(n,this._root);return t.length<2?[]:t[t.length-2].children.map(o=>o.value).filter(o=>o!==n)}pathFromRoot(n){return Ym(n,this._root).map(t=>t.value)}};function Zm(e,n){if(e===n.value)return n;for(let t of n.children){let r=Zm(e,t);if(r)return r}return null}function Ym(e,n){if(e===n.value)return[n];for(let t of n.children){let r=Ym(e,t);if(r.length)return r.unshift(n),r}return[]}var pt=class{value;children;constructor(n,t){this.value=n,this.children=t}toString(){return`TreeNode(${this.value})`}};function ei(e){let n={};return e&&e.children.forEach(t=>n[t.value.outlet]=t),n}var Da=class extends gl{snapshot;constructor(n,t){super(n),this.snapshot=t,ap(this,n)}toString(){return this.snapshot.toString()}};function v_(e,n){let t=Gk(e,n),r=new ze([new gr("",{})]),o=new ze({}),i=new ze({}),a=new ze({}),s=new ze(""),c=new cn(r,o,a,s,i,U,e,t.root);return c.snapshot=t.root,new Da(new pt(c,[]),t)}function Gk(e,n){let t={},r={},o={},a=new ri([],t,o,"",r,U,e,null,{},n);return new Ia("",new pt(a,[]))}var cn=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;resources;_localInjector;pending;paramsSignal;queryParamsSignal;paramMapSignal;queryParamMapSignal;fragmentSignal;dataSignal;constructor(n,t,r,o,i,a,s,c){this.urlSubject=n,this.paramsSubject=t,this.queryParamsSubject=r,this.fragmentSubject=o,this.dataSubject=i,this.outlet=a,this.component=s,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(be(l=>l[Ra]))??X(void 0),this.url=n,this.params=t,this.queryParams=r,this.fragment=o,this.data=i}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(be(n=>co(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(be(n=>co(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}_setPending(n){this._futureSnapshot=n,this.pending?.set(!0)}},Wk="always";function ip(e,n,t){let r,{routeConfig:o}=e;return n!==null&&(t==="always"||o?.path===""||!n.component&&!n.routeConfig?.loadComponent)?r={params:Object.keys(e.params).length===0?n.params:Object.freeze(v(v({},n.params),e.params)),data:Object.freeze(v(v({},n.data),e.data)),resolve:v(v(v(v({},e.data),n.data),o?.data),e._resolvedData)}:r={params:Object.freeze(v({},e.params)),data:Object.freeze(v({},e.data)),resolve:v(v({},e.data),e._resolvedData??{})},o&&x_(o)&&(r.resolve[Ra]=o.title),r}var ri=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;resources;get title(){return this.data?.[Ra]}constructor(n,t,r,o,i,a,s,c,l,d){this.url=n,this.params=t,this.queryParams=r,this.fragment=o,this.data=i,this.outlet=a,this.component=s,this.routeConfig=c,this._resolve=l,this._environmentInjector=d}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=co(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=co(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(r=>r.toString()).join("/"),t=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${t}')`}},Ia=class extends gl{url;constructor(n,t){super(t),this.url=n,ap(this,t)}toString(){return __(this._root)}};function ap(e,n){n.value._routerState=e,n.children.forEach(t=>ap(e,t))}function __(e){let n=e.children.length>0?` { ${e.children.map(__).join(", ")} } `:"";return`${e.value}${n}`}function Hm(e){if(e.snapshot){let n=e.snapshot,t=e._futureSnapshot;e.snapshot=t,kn(n.queryParams,t.queryParams)||e.queryParamsSubject.next(t.queryParams),n.fragment!==t.fragment&&e.fragmentSubject.next(t.fragment),kn(n.params,t.params)||e.paramsSubject.next(t.params),vk(n.url,t.url)||e.urlSubject.next(t.url),kn(n.data,t.data)||e.dataSubject.next(t.data)}else e.snapshot=e._futureSnapshot,e.dataSubject.next(e._futureSnapshot.data)}function Qm(e,n){let t=kn(e.params,n.params)&&wk(e.url,n.url),r=!e.parent!=!n.parent;return t&&!r&&(!e.parent||Qm(e.parent,n.parent))}function x_(e){return typeof e.title=="string"||e.title===null}var C_=new C(""),Fa=(()=>{class e{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=U;activateEvents=new de;deactivateEvents=new de;attachEvents=new de;detachEvents=new de;routerOutletData=En();parentContexts=f(lo);location=f(ur);changeDetector=f(mt);inputBinder=f(Ml,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(t){if(t.name){let{firstChange:r,previousValue:o}=t.name;if(r)return;this.isTrackedInParentContexts(o)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(o)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(t){return this.parentContexts.getContext(t)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let t=this.parentContexts.getContext(this.name);t?.route&&(t.attachRef?this.attach(t.attachRef,t.route):this.activateWith(t.route,t.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new I(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new I(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new I(4012,!1);this.location.detach();let t=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(t.instance),t}attach(t,r){this.activated=t,this._activatedRoute=r,this.location.insert(t.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this,this.location.injector),this.attachEvents.emit(t.instance)}deactivate(){if(this.activated){let t=this.component;this.inputBinder?.unsubscribeFromRouteData(this),this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(t)}}activateWith(t,r){if(this.isActivated)throw new I(4013,!1);this._activatedRoute=t;let o=this.location,a=t.snapshot.component,s=this.parentContexts.getOrCreateContext(this.name).children,c=new Xm(t,s,o.injector,this.routerOutletData);this.activated=o.createComponent(a,{index:o.length,injector:c,environmentInjector:r}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this,this.location.injector),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(r){return new(r||e)};static \u0275dir=re({type:e,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[yt]})}return e})(),Xm=class{route;childContexts;parent;outletData;constructor(n,t,r,o){this.route=n,this.childContexts=t,this.parent=r,this.outletData=o}get(n,t){return n===cn?this.route:n===lo?this.childContexts:n===C_?this.outletData:this.parent.get(n,t)}},Ml=new C("");var sp=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275cmp=Q({type:e,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(r,o){r&1&&oe(0,"router-outlet")},dependencies:[Fa],encapsulation:2,changeDetection:1})}return e})();function cp(e){let n=e.children&&e.children.map(cp),t=n?T(v({},e),{children:n}):v({},e);return!t.component&&!t.loadComponent&&(n||t.loadChildren)&&t.outlet&&t.outlet!==U&&(t.component=sp),t}var Ca="imperative",Ge=(function(e){return e[e.NavigationStart=0]="NavigationStart",e[e.NavigationEnd=1]="NavigationEnd",e[e.NavigationCancel=2]="NavigationCancel",e[e.NavigationError=3]="NavigationError",e[e.RoutesRecognized=4]="RoutesRecognized",e[e.ResolveStart=5]="ResolveStart",e[e.ResolveEnd=6]="ResolveEnd",e[e.GuardsCheckStart=7]="GuardsCheckStart",e[e.GuardsCheckEnd=8]="GuardsCheckEnd",e[e.RouteConfigLoadStart=9]="RouteConfigLoadStart",e[e.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",e[e.ChildActivationStart=11]="ChildActivationStart",e[e.ChildActivationEnd=12]="ChildActivationEnd",e[e.ActivationStart=13]="ActivationStart",e[e.ActivationEnd=14]="ActivationEnd",e[e.Scroll=15]="Scroll",e[e.NavigationSkipped=16]="NavigationSkipped",e})(Ge||{}),Tt=class{id;url;constructor(n,t){this.id=n,this.url=t}},uo=class extends Tt{type=Ge.NavigationStart;navigationTrigger;restoredState;constructor(n,t,r="imperative",o=null){super(n,t),this.navigationTrigger=r,this.restoredState=o}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},Nn=class extends Tt{urlAfterRedirects;type=Ge.NavigationEnd;constructor(n,t,r){super(n,t),this.urlAfterRedirects=r}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},Xe=(function(e){return e[e.Redirect=0]="Redirect",e[e.SupersededByNewNavigation=1]="SupersededByNewNavigation",e[e.NoDataFromResolver=2]="NoDataFromResolver",e[e.GuardRejected=3]="GuardRejected",e[e.Aborted=4]="Aborted",e})(Xe||{}),ka=(function(e){return e[e.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",e[e.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",e})(ka||{}),Wt=class extends Tt{reason;code;type=Ge.NavigationCancel;constructor(n,t,r,o){super(n,t),this.reason=r,this.code=o}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function S_(e){return e instanceof Wt&&(e.code===Xe.Redirect||e.code===Xe.SupersededByNewNavigation)}var Kn=class extends Tt{reason;code;type=Ge.NavigationSkipped;constructor(n,t,r,o){super(n,t),this.reason=r,this.code=o}},fo=class extends Tt{error;target;type=Ge.NavigationError;constructor(n,t,r,o){super(n,t),this.error=r,this.target=o}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},Ma=class extends Tt{urlAfterRedirects;state;type=Ge.RoutesRecognized;constructor(n,t,r,o){super(n,t),this.urlAfterRedirects=r,this.state=o}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},bl=class extends Tt{urlAfterRedirects;state;type=Ge.GuardsCheckStart;constructor(n,t,r,o){super(n,t),this.urlAfterRedirects=r,this.state=o}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},yl=class extends Tt{urlAfterRedirects;state;shouldActivate;type=Ge.GuardsCheckEnd;constructor(n,t,r,o,i){super(n,t),this.urlAfterRedirects=r,this.state=o,this.shouldActivate=i}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},vl=class extends Tt{urlAfterRedirects;state;type=Ge.ResolveStart;constructor(n,t,r,o){super(n,t),this.urlAfterRedirects=r,this.state=o}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},_l=class extends Tt{urlAfterRedirects;state;type=Ge.ResolveEnd;constructor(n,t,r,o){super(n,t),this.urlAfterRedirects=r,this.state=o}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},xl=class{route;type=Ge.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},Cl=class{route;type=Ge.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},Sl=class{snapshot;type=Ge.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},wl=class{snapshot;type=Ge.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},El=class{snapshot;type=Ge.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Dl=class{snapshot;type=Ge.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var oi=class{},Na=class{},ii=class{url;navigationBehaviorOptions;constructor(n,t){this.url=n,this.navigationBehaviorOptions=t}};function qk(e){return!(e instanceof oi)&&!(e instanceof ii)&&!(e instanceof Na)}function Kk(e,n,t){let r=new Set,o=Ta(e,n._root,t?t._root:void 0,r);return{newlyCreatedRoutes:r,state:new Da(o,n)}}function Ta(e,n,t,r){if(t&&e.shouldReuseRoute(n.value,t.value.snapshot)){let o=t.value;o._setPending(n.value);let i=Zk(e,n,t,r);return new pt(o,i)}else{if(e.shouldAttach(n.value)){let a=e.retrieve(n.value);if(a!==null){let s=a.route;return s.value._setPending(n.value),s.children=n.children.map(c=>Ta(e,c,void 0,r)),s}}let o=Yk(n.value);o._setPending(n.value),r.add(o);let i=n.children.map(a=>Ta(e,a,void 0,r));return new pt(o,i)}}function Zk(e,n,t,r){return n.children.map(o=>{for(let i of t.children)if(e.shouldReuseRoute(o.value,i.value.snapshot))return Ta(e,o,i,r);return Ta(e,o,void 0,r)})}function Yk(e){return new cn(new ze(e.url),new ze(e.params),new ze(e.queryParams),new ze(e.fragment),new ze(e.data),e.outlet,e.component,e)}var vr=class e extends Error{redirectTo;navigationBehaviorOptions;constructor(n,t){super(),this.redirectTo=n,this.navigationBehaviorOptions=t,Object.setPrototypeOf(this,e.prototype)}},w_="ngNavigationCancelingError";function Sa(e,n){let{redirectTo:t,navigationBehaviorOptions:r}=yr(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,o=E_(!1,Xe.Redirect);return o.url=t,o.navigationBehaviorOptions=r,o}function E_(e,n){let t=new Error(`NavigationCancelingError: ${e||""}`);return t[w_]=!0,t.cancellationCode=n,t}function Qk(e){return D_(e)&&yr(e.url)}function D_(e){return!!e&&e[w_]}var Jm=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,t,r,o,i){this.routeReuseStrategy=n,this.futureState=t,this.currState=r,this.forwardEvent=o,this.inputBindingEnabled=i}activate(n){let t=this.futureState._root,r=this.currState?this.currState._root:null;this.deactivateChildRoutes(t,r,n),Hm(this.futureState.root),this.activateChildRoutes(t,r,n)}deactivateChildRoutes(n,t,r){let o=ei(t);n.children.forEach(i=>{let a=i.value.outlet;this.deactivateRoutes(i,o[a],r),delete o[a]}),Object.values(o).forEach(i=>{this.deactivateRouteAndItsChildren(i,r)})}deactivateRoutes(n,t,r){let o=n.value,i=t?t.value:null;if(o===i)if(o.component){let a=r.getContext(o.outlet);a&&this.deactivateChildRoutes(n,t,a.children)}else this.deactivateChildRoutes(n,t,r);else i&&this.deactivateRouteAndItsChildren(t,r)}deactivateRouteAndItsChildren(n,t){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,t):this.deactivateRouteAndOutlet(n,t)}detachAndStoreRouteSubtree(n,t){let r=t.getContext(n.value.outlet),o=r&&n.value.component?r.children:t,i=ei(n);for(let a of Object.values(i))this.deactivateRouteAndItsChildren(a,o);if(r&&r.outlet){let a=r.outlet.detach(),s=r.children.contexts;r.resetChildren(),this.routeReuseStrategy.store(n.value.snapshot,{componentRef:a,route:n,contexts:s})}}deactivateRouteAndOutlet(n,t){let r=t.getContext(n.value.outlet),o=r&&n.value.component?r.children:t,i=ei(n);for(let a of Object.values(i))this.deactivateRouteAndItsChildren(a,o);r&&(r.outlet&&(r.outlet.deactivate(),r.children.onOutletDeactivated()),r.attachRef=null,r.route=null),n.value._localInjector?.destroy()}activateChildRoutes(n,t,r){let o=ei(t);n.children.forEach(i=>{this.activateRoutes(i,o[i.value.outlet],r),this.forwardEvent(new Dl(i.value.snapshot))}),n.children.length&&this.forwardEvent(new wl(n.value.snapshot))}activateRoutes(n,t,r){let o=n.value,i=t?t.value:null;if(Hm(o),o===i)if(o.component){let a=r.getOrCreateContext(o.outlet);this.activateChildRoutes(n,t,a.children)}else this.activateChildRoutes(n,t,r);else if(o.component){let a=r.getOrCreateContext(o.outlet);if(this.routeReuseStrategy.shouldAttach(o.snapshot)){let s=this.routeReuseStrategy.retrieve(o.snapshot);this.routeReuseStrategy.store(o.snapshot,null),a.children.onOutletReAttached(s.contexts),a.attachRef=s.componentRef,a.route=s.route.value,a.outlet&&a.outlet.attach(s.componentRef,s.route.value),Hm(s.route.value),this.activateChildRoutes(n,null,a.children)}else a.attachRef=null,a.route=o,a.outlet&&a.outlet.activateWith(o,a.injector),this.activateChildRoutes(n,null,a.children)}else this.activateChildRoutes(n,null,r)}},Il=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},ni=class{component;route;constructor(n,t){this.component=n,this.route=t}};function Xk(e,n,t){let r=e._root,o=n?n._root:null;return _a(r,o,t,[r.value])}function Jk(e){let n=e.routeConfig?e.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:e,guards:n}}function ci(e,n){let t=Symbol(),r=n.get(e,t);return r===t?typeof e=="function"&&!lu(e)?e:n.get(e):r}function _a(e,n,t,r,o={canDeactivateChecks:[],canActivateChecks:[]}){let i=ei(n);return e.children.forEach(a=>{eM(a,i[a.value.outlet],t,r.concat([a.value]),o),delete i[a.value.outlet]}),Object.entries(i).forEach(([a,s])=>wa(s,t.getContext(a),t,o)),o}function eM(e,n,t,r,o={canDeactivateChecks:[],canActivateChecks:[]}){let i=e.value,a=n?n.value:null,s=t?t.getContext(e.value.outlet):null;if(a&&i.routeConfig===a.routeConfig){let c=tM(a,i,i.routeConfig.runGuardsAndResolvers);c?o.canActivateChecks.push(new Il(r)):(i.data=a.data,i._resolvedData=a._resolvedData),i.component?_a(e,n,s?s.children:null,r,o):_a(e,n,t,r,o),c&&s&&s.outlet&&s.outlet.isActivated&&o.canDeactivateChecks.push(new ni(s.outlet.component,a))}else a&&wa(n,s,t,o),o.canActivateChecks.push(new Il(r)),i.component?_a(e,null,s?s.children:null,r,o):_a(e,null,t,r,o);return o}function tM(e,n,t){if(typeof t=="function")return Ke(n._environmentInjector,()=>t(e,n));switch(t){case"pathParamsChange":return!so(e.url,n.url);case"pathParamsOrQueryParamsChange":return!so(e.url,n.url)||!kn(e.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!Qm(e,n)||!kn(e.queryParams,n.queryParams);default:return!Qm(e,n)}}function wa(e,n,t,r){let o=ei(e),i=e.value;Object.entries(o).forEach(([a,s])=>{i.component?n?wa(s,n.children.getContext(a),n.children,r):wa(s,null,null,r):wa(s,t?t.getContext(a):null,t,r)}),i.component?n&&n.outlet&&n.outlet.isActivated?r.canDeactivateChecks.push(new ni(n.outlet.component,i)):r.canDeactivateChecks.push(new ni(null,i)):r.canDeactivateChecks.push(new ni(null,i))}function Pa(e){return typeof e=="function"}function nM(e){return typeof e=="boolean"}function rM(e){return e&&Pa(e.canLoad)}function oM(e){return e&&Pa(e.canActivate)}function iM(e){return e&&Pa(e.canActivateChild)}function aM(e){return e&&Pa(e.canDeactivate)}function sM(e){return e&&Pa(e.canMatch)}function I_(e){return e instanceof Nr||e?.name==="EmptyError"}var al=Symbol("INITIAL_VALUE");function ai(){return Ot(e=>Ld(e.map(n=>n.pipe(er(1),Do(t=>{if(t instanceof vr)return X(t);throw t}),Ti(al)))).pipe(be(n=>{for(let t of n)if(t!==!0){if(t===al)return al;if(t===!1||cM(t))return t}return!0}),dt(n=>n!==al),er(1)))}function cM(e){return yr(e)||e instanceof vr}function k_(e){return new Z(n=>{if(e.aborted){n.next(),n.complete();return}let t=()=>{n.next(),n.complete()};return e.addEventListener("abort",t),()=>e.removeEventListener("abort",t)})}function M_(e){return Jt(k_(e))}function lM(e){return lt(n=>{let{targetSnapshot:t,currentSnapshot:r,guards:{canActivateChecks:o,canDeactivateChecks:i}}=n;return i.length===0&&o.length===0?X(T(v({},n),{guardsResult:!0})):dM(i,t,r).pipe(lt(a=>a&&nM(a)?uM(t,o,e):X(a)),be(a=>T(v({},n),{guardsResult:a})))})}function dM(e,n,t){return we(e).pipe(lt(r=>gM(r.component,r.route,t,n)),Fn(r=>r!==!0,!0))}function uM(e,n,t){return we(n).pipe(Fs(r=>Eo(mM(r.route.parent,t),fM(r.route,t),hM(e,r.path),pM(e,r.route))),Fn(r=>r!==!0,!0))}function fM(e,n){return e!==null&&n&&n(new El(e)),X(!0)}function mM(e,n){return e!==null&&n&&n(new Sl(e)),X(!0)}function pM(e,n){let t=n.routeConfig?n.routeConfig.canActivate:null;if(!t||t.length===0)return X(!0);let r=t.map(o=>Ni(()=>{let i=n._environmentInjector,a=ci(o,i),s=oM(a)?a.canActivate(n,e):Ke(i,()=>a(n,e));return Oa(s).pipe(Fn())}));return X(r).pipe(ai())}function hM(e,n){let t=n[n.length-1],o=n.slice(0,n.length-1).reverse().map(i=>Jk(i)).filter(i=>i!==null).map(i=>Ni(()=>{let a=i.guards.map(s=>{let c=i.node._environmentInjector,l=ci(s,c),d=iM(l)?l.canActivateChild(t,e):Ke(c,()=>l(t,e));return Oa(d).pipe(Fn())});return X(a).pipe(ai())}));return X(o).pipe(ai())}function gM(e,n,t,r){let o=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!o||o.length===0)return X(!0);let i=o.map(a=>{let s=n._environmentInjector,c=ci(a,s);return lp(()=>aM(c)?c.canDeactivate(e,n,t,r):Ke(s,()=>c(e,n,t,r))).pipe(Fn())});return X(i).pipe(ai())}function bM(e,n,t,r,o){let i=n.canLoad;if(i===void 0||i.length===0)return X(!0);let a=i.map(s=>{let c=ci(s,e),l=lp(()=>rM(c)?c.canLoad(n,t):Ke(e,()=>c(n,t)));return o?l.pipe(M_(o)):l});return X(a).pipe(ai(),N_(r))}function lp(e){try{return Oa(e())}catch(n){return new Z(t=>t.error(n))}}function N_(e){return ps(Ft(n=>{if(typeof n!="boolean")throw Sa(e,n)}),be(n=>n===!0))}function yM(e,n,t,r,o,i){let a=n.canMatch;if(!a||a.length===0)return X(!0);let s=a.map(c=>{let l=ci(c,e);return lp(()=>sM(l)?l.canMatch(n,t,o):Ke(e,()=>l(n,t,o))).pipe(M_(i))});return X(s).pipe(ai(),N_(r))}var In=class e extends Error{name="NoMatch";segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,e.prototype)}},Aa=class e extends Error{urlTree;name="AbsoluteRedirect";constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,e.prototype)}};function vM(e){throw new I(4e3,!1)}function _M(e){throw E_(!1,Xe.GuardRejected)}var ep=class{urlSerializer;urlTree;constructor(n,t){this.urlSerializer=n,this.urlTree=t}async lineralizeSegments(n,t){let r=[],o=t.root;for(;;){if(r=r.concat(o.segments),o.numberOfChildren===0)return r;if(o.numberOfChildren>1||!o.children[U])throw vM(`${n.redirectTo}`);o=o.children[U]}}async applyRedirectCommands(n,t,r,o,i){let a=await xM(t,o,i);if(a instanceof Nt)throw new Aa(a);let s=this.applyRedirectCreateUrlTree(a,this.urlSerializer.parse(a),n,r);if(a[0]==="/")throw new Aa(s);return s}applyRedirectCreateUrlTree(n,t,r,o){let i=this.createSegmentGroup(n,t.root,r,o);return new Nt(i,this.createQueryParams(t.queryParams,this.urlTree.queryParams),t.fragment)}createQueryParams(n,t){let r={};return Object.entries(n).forEach(([o,i])=>{if(typeof i=="string"&&i[0]===":"){let s=i.substring(1);r[o]=t[s]}else r[o]=i}),r}createSegmentGroup(n,t,r,o){let i=this.createSegments(n,t.segments,r,o),a=Object.create(null);return Object.entries(t.children).forEach(([s,c])=>{a[s]=this.createSegmentGroup(n,c,r,o)}),new me(i,a)}createSegments(n,t,r,o){return t.map(i=>i.path[0]===":"?this.findPosParam(n,i,o):this.findOrReturn(i,r))}findPosParam(n,t,r){let o=r[t.path.substring(1)];if(!o)throw new I(4001,!1);return o}findOrReturn(n,t){let r=0;for(let o of t){if(o.path===n.path)return t.splice(r),o;r++}return n}};function xM(e,n,t){if(typeof e=="string")return Promise.resolve(e);let r=e;return dl(Oa(Ke(t,()=>r(n))))}function CM(e,n){return e.providers&&!e._injector&&(e._injector=ua(e.providers,n,`Route: ${e.path}`)),e._injector??n}function Mn(e){return e.outlet||U}function SM(e,n){let t=e.filter(r=>Mn(r)===n);return t.push(...e.filter(r=>Mn(r)!==n)),t}var tp={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function T_(e){return{routeConfig:e.routeConfig,url:e.url,params:e.params,queryParams:e.queryParams,fragment:e.fragment,data:e.data,outlet:e.outlet,title:e.title,paramMap:e.paramMap,queryParamMap:e.queryParamMap}}function wM(e,n,t,r,o,i,a){let s=A_(e,n,t);if(!s.matched)return X(s);let c=T_(i(s));return r=CM(n,r),yM(r,n,t,o,c,a).pipe(be(l=>l===!0?s:v({},tp)))}function A_(e,n,t){if(n.path==="")return n.pathMatch==="full"&&(e.hasChildren()||t.length>0)?v({},tp):{matched:!0,consumedSegments:[],remainingSegments:t,parameters:{},positionalParamSegments:{}};let o=(n.matcher||t_)(t,e,n);if(!o)return v({},tp);let i={};Object.entries(o.posParams??{}).forEach(([s,c])=>{i[s]=c.path});let a=o.consumed.length>0?v(v({},i),o.consumed[o.consumed.length-1].parameters):i;return{matched:!0,consumedSegments:o.consumed,remainingSegments:t.slice(o.consumed.length),parameters:a,positionalParamSegments:o.posParams??{}}}function Jv(e,n,t,r){return t.length>0&&IM(e,t,r)?{segmentGroup:new me(n,DM(r,new me(t,e.children))),slicedSegments:[]}:t.length===0&&kM(e,t,r)?{segmentGroup:new me(e.segments,EM(e,t,r,e.children)),slicedSegments:t}:{segmentGroup:new me(e.segments,e.children),slicedSegments:t}}function EM(e,n,t,r){let o={};for(let i of t)if(Nl(e,n,i)&&!r[Mn(i)]){let a=new me([],{});o[Mn(i)]=a}return v(v({},r),o)}function DM(e,n){let t={};t[U]=n;for(let r of e)if(r.path===""&&Mn(r)!==U){let o=new me([],{});t[Mn(r)]=o}return t}function IM(e,n,t){return t.some(r=>Nl(e,n,r)&&Mn(r)!==U)}function kM(e,n,t){return t.some(r=>Nl(e,n,r))}function Nl(e,n,t){return(e.hasChildren()||n.length>0)&&t.pathMatch==="full"?!1:t.path===""}function MM(e,n,t){return n.length===0&&!e.hasChildren()}var np=class{};async function NM(e,n,t,r,o,i,a,s){return new rp(e,n,t,r,o,a,i,s).recognize()}var TM=31,rp=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;queryParams;constructor(n,t,r,o,i,a,s,c){this.injector=n,this.configLoader=t,this.rootComponentType=r,this.config=o,this.urlTree=i,this.paramsInheritanceStrategy=a,this.urlSerializer=s,this.abortSignal=c,this.applyRedirects=new ep(this.urlSerializer,this.urlTree),this.queryParams=Object.freeze(v({},this.urlTree.queryParams))}noMatchError(n){return new I(4002,`'${n.segmentGroup}'`)}async recognize(){let n=Jv(this.urlTree.root,[],[],this.config).segmentGroup,{children:t,rootSnapshot:r}=await this.match(n),o=new pt(r,t),i=new Ia("",o),a=p_(r,[],this.urlTree.queryParams,this.urlTree.fragment);return a.queryParams=this.urlTree.queryParams,i.url=this.urlSerializer.serialize(a),{state:i,tree:a}}async match(n){let t=new ri([],Object.freeze({}),this.queryParams,this.urlTree.fragment,Object.freeze({}),U,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,U,t),rootSnapshot:t}}catch(r){if(r instanceof Aa)return this.absoluteRedirectCount++,this.absoluteRedirectCount>TM&&(this.allowRedirects=!1),this.urlTree=r.urlTree,this.queryParams=Object.freeze(v({},this.urlTree.queryParams)),this.match(r.urlTree.root);throw r instanceof In?this.noMatchError(r):r}}async processSegmentGroup(n,t,r,o,i){if(r.segments.length===0&&r.hasChildren())return this.processChildren(n,t,r,i);let a=await this.processSegment(n,t,r,r.segments,o,!0,i);return a instanceof pt?[a]:[]}async processChildren(n,t,r,o){let i=[];for(let c of Object.keys(r.children))c==="primary"?i.unshift(c):i.push(c);let a=[];for(let c of i){let l=r.children[c],d=SM(t,c),u=await this.processSegment(n,d,l,l.segments,c,!0,o);u instanceof pt&&a.push(u)}let s=R_(a);return AM(s),s}async processSegment(n,t,r,o,i,a,s){for(let c of t)try{return await this.processSegmentAgainstRoute(c._injector??n,t,c,r,o,i,a,s)}catch(l){if(l instanceof In||I_(l))continue;throw l}if(MM(r,o))return new np;throw new In(r)}async processSegmentAgainstRoute(n,t,r,o,i,a,s,c){if(Mn(r)!==a&&(a===U||!Nl(o,i,r)||!r.children?.length&&!r.loadChildren||i.length===0&&!o.hasChildren()))throw new In(o);if(r.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,o,r,i,a,c);if(this.allowRedirects&&s)return this.expandSegmentAgainstRouteUsingRedirect(n,o,t,r,i,a,c);throw new In(o)}async expandSegmentAgainstRouteUsingRedirect(n,t,r,o,i,a,s){let{matched:c,parameters:l,consumedSegments:d,positionalParamSegments:u,remainingSegments:g}=A_(t,o,i);if(!c)throw new In(t);let h=this.createSnapshot(n,o,i,l,s);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let _=await this.applyRedirects.applyRedirectCommands(d,o.redirectTo,u,T_(h),n),D=await this.applyRedirects.lineralizeSegments(o,_);return this.processSegment(n,r,t,D.concat(g),a,!1,s)}createSnapshot(n,t,r,o,i){let a=new ri(r,o,this.queryParams,this.urlTree.fragment,FM(t),Mn(t),t.component??t._loadedComponent??null,t,PM(t),n),s=ip(a,i,this.paramsInheritanceStrategy);return a.params=s.params,a.data=s.data,a}async matchSegmentAgainstRoute(n,t,r,o,i,a){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let s=Je=>this.createSnapshot(n,r,Je.consumedSegments,Je.parameters,a),c=await dl(wM(t,r,o,n,this.urlSerializer,s,this.abortSignal));if(r.path==="**"&&(t.children={}),!c?.matched)throw new In(t);n=r._injector??n;let{routes:l}=await this.getChildConfig(n,r,o),d=r._loadedInjector??n,{parameters:u,consumedSegments:g,remainingSegments:h}=c,_=this.createSnapshot(n,r,g,u,a),D=Mn(r)===i,{segmentGroup:A,slicedSegments:j}=D?Jv(t,g,h,l):{segmentGroup:t,slicedSegments:h};if(D&&j.length===0&&A.hasChildren()){let Je=await this.processChildren(d,l,A,_);return new pt(_,Je)}if(D&&l.length===0&&j.length===0)return new pt(_,[]);let J=await this.processSegment(d,l,A,j,D?U:i,!0,_);if(!D&&!(J instanceof pt))throw new In(t);return new pt(_,J instanceof pt?[J]:[])}async getChildConfig(n,t,r){if(t.children)return{routes:t.children,injector:n};if(t.loadChildren){if(t._loadedRoutes!==void 0){let i=t._loadedNgModuleFactory;return i&&!t._loadedInjector&&(t._loadedInjector=i.create(n).injector),{routes:t._loadedRoutes,injector:t._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await dl(bM(n,t,r,this.urlSerializer,this.abortSignal))){let i=await this.configLoader.loadChildren(n,t);return t._loadedRoutes=i.routes,t._loadedInjector=i.injector,t._loadedNgModuleFactory=i.factory,i}throw _M(t)}return{routes:[],injector:n}}};function AM(e){e.sort((n,t)=>n.value.outlet===U?-1:t.value.outlet===U?1:n.value.outlet.localeCompare(t.value.outlet))}function RM(e){let n=e.value.routeConfig;return n&&n.path===""}function R_(e){let n=[],t=new Set;for(let o of e){if(!RM(o)){n.push(o);continue}let i=n.find(a=>o.value.routeConfig===a.value.routeConfig);i!==void 0?(i.children.push(...o.children),t.add(i)):n.push(o)}for(let o of t){let i=R_(o.children);n.push(new pt(o.value,i))}let r=n.filter(o=>!t.has(o));return OM(r),r}function OM(e){let n=Object.create(null);e.forEach(t=>{let r=n[t.value.outlet];if(r){let o=r.url.map(a=>a.toString()).join("/"),i=t.value.url.map(a=>a.toString()).join("/");throw new I(4006,!1)}n[t.value.outlet]=t.value})}function FM(e){return e.data||{}}function PM(e){return e.resolve||{}}function LM(e,n,t,r,o,i,a){return lt(async s=>{let{state:c,tree:l}=await NM(e,n,t,r,s.extractedUrl,o,i,a);return T(v({},s),{targetSnapshot:c,urlAfterRedirects:l})})}function VM(e){return lt(n=>{let{targetSnapshot:t,guards:{canActivateChecks:r}}=n;if(!r.length)return X(n);let o=new Set(r.map(s=>s.route)),i=new Set;for(let s of o)if(!i.has(s))for(let c of O_(s))i.add(c);let a=0;return we(i).pipe(Fs(s=>o.has(s)?jM(s,t,e):(s.data=ip(s,s.parent,e).resolve,X(void 0))),Ft(()=>a++),Ps(1),lt(s=>a===i.size?X(n):Fe))})}function O_(e){let n=e.children.map(t=>O_(t)).flat();return[e,...n]}function jM(e,n,t){let r=e.routeConfig,o=e._resolve;return r?.title!==void 0&&!x_(r)&&(o[Ra]=r.title),Ni(()=>(e.data=ip(e,e.parent,t).resolve,BM(o,e,n).pipe(be(i=>(e._resolvedData=i,e.data=v(v({},e.data),i),null)))))}function BM(e,n,t){let r=$m(e);if(r.length===0)return X({});let o={};return we(r).pipe(lt(i=>UM(e[i],n,t).pipe(Fn(),Ft(a=>{if(a instanceof vr)throw Sa(new br,a);o[i]=a}))),Ps(1),be(()=>o),Do(i=>I_(i)?Fe:Pd(i)))}function UM(e,n,t){let r=n._environmentInjector,o=ci(e,r),i=o.resolve?o.resolve(n,t):Ke(r,()=>o(n,t));return Oa(i)}var F_=new C("");function op(e){return Ot(n=>{let t=e(n);return t?we(t).pipe(be(()=>n)):X(n)})}var dp=(()=>{class e{buildTitle(t){let r,o=t.root;for(;o!==void 0;)r=this.getResolvedTitleForRoute(o)??r,o=o.children.find(i=>i.outlet===U);return r}getResolvedTitleForRoute(t){return t.data[Ra]}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:()=>f(P_)})}return e})(),P_=(()=>{class e extends dp{title;constructor(t){super(),this.title=t}updateTitle(t){let r=this.buildTitle(t);r!==void 0&&this.title.setTitle(r)}static \u0275fac=function(r){return new(r||e)(H(qv))};static \u0275prov=G({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),li=new C("",{factory:()=>({})}),La=new C(""),L_=(()=>{class e{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=f(gm);async loadComponent(t,r){if(this.componentLoaders.get(r))return this.componentLoaders.get(r);if(r._loadedComponent)return Promise.resolve(r._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(r);let o=(async()=>{try{let i=await r_(Ke(t,()=>r.loadComponent())),a=await j_(Cm(i));return this.onLoadEndListener&&this.onLoadEndListener(r),r._loadedComponent=a,a}finally{this.componentLoaders.delete(r)}})();return this.componentLoaders.set(r,o),o}loadChildren(t,r){if(this.childrenLoaders.get(r))return this.childrenLoaders.get(r);if(r._loadedRoutes)return Promise.resolve({routes:r._loadedRoutes,injector:r._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(r);let o=(async()=>{try{let i=await V_(r,this.compiler,t,this.onLoadEndListener);return r._loadedRoutes=i.routes,r._loadedInjector=i.injector,r._loadedNgModuleFactory=i.factory,i}finally{this.childrenLoaders.delete(r)}})();return this.childrenLoaders.set(r,o),o}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})();async function V_(e,n,t,r){let o=await r_(Ke(t,()=>e.loadChildren())),i=await j_(Cm(o)),a;i instanceof zc||Array.isArray(i)?a=i:a=await n.compileModuleAsync(i),r&&r(e);let s,c,l=!1,d;return Array.isArray(a)?(c=a,l=!0):(s=a.create(t).injector,d=a,c=s.get(La,[],{optional:!0,self:!0}).flat()),{routes:c.map(cp),injector:s,factory:d}}async function j_(e){return e}var Tl=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:()=>f(HM)})}return e})(),HM=(()=>{class e{shouldProcessUrl(t){return!0}extract(t){return t}merge(t,r){return t}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})(),B_=new C("");var zM=()=>{},U_=new C(""),H_=(()=>{class e{currentNavigation=E(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=E(null);events=new q;transitionAbortWithErrorSubject=new q;configLoader=f(L_);environmentInjector=f(Pe);destroyRef=f(tt);urlSerializer=f(si);rootContexts=f(lo);location=f(ao);inputBindingEnabled=f(Ml,{optional:!0})!==null;titleStrategy=f(dp);options=f(li,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||Wk;urlHandlingStrategy=f(Tl);createViewTransition=f(B_,{optional:!0});navigationErrorHandler=f(U_,{optional:!0});routerResourcesFeature=f(F_,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>X(void 0);rootComponentType=null;destroyed=!1;constructor(){let t=o=>this.events.next(new xl(o)),r=o=>this.events.next(new Cl(o));this.configLoader.onLoadEndListener=r,this.configLoader.onLoadStartListener=t,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(t){let r=++this.navigationId;te(()=>{this.transitions?.next(T(v({},t),{extractedUrl:this.urlHandlingStrategy.extract(t.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:r,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(t){return this.transitions=new ze(null),this.transitions.pipe(dt(r=>r!==null),Ot(r=>{let o=!0,i=!1,a=new AbortController,s=()=>!i&&this.currentTransition?.id===r.id;return X(r).pipe(Ot(c=>{if(this.navigationId>r.id)return this.cancelNavigationTransition(r,"",Xe.SupersededByNewNavigation),Fe;this.currentTransition=r;let l=this.lastSuccessfulNavigation();this.currentNavigation.set({id:c.id,initialUrl:c.rawUrl,extractedUrl:c.extractedUrl,targetBrowserUrl:typeof c.extras.browserUrl=="string"?this.urlSerializer.parse(c.extras.browserUrl):c.extras.browserUrl,trigger:c.source,extras:c.extras,previousNavigation:l?T(v({},l),{previousNavigation:null}):null,abort:()=>a.abort(),routesRecognizeHandler:c.routesRecognizeHandler,beforeActivateHandler:c.beforeActivateHandler});let d=!t.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),u=c.extras.onSameUrlNavigation??t.onSameUrlNavigation;if(!d&&u!=="reload")return this.events.next(new Kn(c.id,this.urlSerializer.serialize(c.rawUrl),"",ka.IgnoredSameUrlNavigation)),c.resolve(!1),Fe;if(this.urlHandlingStrategy.shouldProcessUrl(c.rawUrl))return X(c).pipe(Ot(g=>(this.events.next(new uo(g.id,this.urlSerializer.serialize(g.extractedUrl),g.source,g.restoredState)),g.id!==this.navigationId?Fe:Promise.resolve(g))),LM(this.environmentInjector,this.configLoader,this.rootComponentType,t.config,this.urlSerializer,this.paramsInheritanceStrategy,a.signal),Ft(g=>{r.targetSnapshot=g.targetSnapshot,r.urlAfterRedirects=g.urlAfterRedirects,this.currentNavigation.update(h=>(h.finalUrl=g.urlAfterRedirects,h)),this.events.next(new Na)}),Ot(g=>we(r.routesRecognizeHandler.deferredHandle??X(void 0)).pipe(be(()=>g))),Ft(()=>{let g=new Ma(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);this.events.next(g)}));if(d&&this.urlHandlingStrategy.shouldProcessUrl(c.currentRawUrl)){let{id:g,extractedUrl:h,source:_,restoredState:D,extras:A}=c,j=new uo(g,this.urlSerializer.serialize(h),_,D);this.events.next(j);let J=v_(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=r=T(v({},c),{targetSnapshot:J,urlAfterRedirects:h,extras:T(v({},A),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(Je=>(Je.finalUrl=h,Je)),X(r)}else return this.events.next(new Kn(c.id,this.urlSerializer.serialize(c.extractedUrl),"",ka.IgnoredByUrlHandlingStrategy)),c.resolve(!1),Fe}),be(c=>{let l=new bl(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);return this.events.next(l),this.currentTransition=r=T(v({},c),{guards:Xk(c.targetSnapshot,c.currentSnapshot,this.rootContexts)}),r}),lM(c=>this.events.next(c)),Ot(c=>{if(r.guardsResult=c.guardsResult,c.guardsResult&&typeof c.guardsResult!="boolean")throw Sa(this.urlSerializer,c.guardsResult);let l=new yl(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot,!!c.guardsResult);if(this.events.next(l),!s())return Fe;if(!c.guardsResult)return this.cancelNavigationTransition(c,"",Xe.GuardRejected),Fe;if(c.guards.canActivateChecks.length===0)return X(c);let d=new vl(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);if(this.events.next(d),!s())return Fe;let u=!1;return X(c).pipe(VM(this.paramsInheritanceStrategy),Ft({next:()=>{u=!0;let g=new _l(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);this.events.next(g)},complete:()=>{u||this.cancelNavigationTransition(c,"",Xe.NoDataFromResolver)}}))}),op(c=>{let l=u=>{let g=[];if(u.routeConfig?._loadedComponent)u.component=u.routeConfig?._loadedComponent;else if(u.routeConfig?.loadComponent){let h=u._environmentInjector;g.push(this.configLoader.loadComponent(h,u.routeConfig).then(_=>{u.component=_}))}for(let h of u.children)g.push(...l(h));return g},d=l(c.targetSnapshot.root);return d.length===0?X(c):we(Promise.all(d).then(()=>c))}),Ot(c=>{let{newlyCreatedRoutes:l,state:d}=Kk(t.routeReuseStrategy,c.targetSnapshot,c.currentRouterState);return this.currentTransition=r=c=T(v({},c),{targetRouterState:d,newlyCreatedRoutes:l}),this.currentNavigation.update(u=>(u.targetRouterState=d,u)),X(c)}),this.routerResourcesFeature?.setupAndRunResources(a.signal)??(c=>c),op(()=>this.afterPreactivation()),Ot(()=>{let{currentSnapshot:c,targetSnapshot:l}=r,d=this.createViewTransition?.(this.environmentInjector,c.root,l.root,r.hasUAVisualTransition);return d?we(d).pipe(be(()=>r)):X(r)}),er(1),Ot(c=>{o=!1,this.events.next(new oi);let l=r.beforeActivateHandler.deferredHandle;return l?we(l.then(()=>c)):X(c)}),Ft(c=>{new Jm(t.routeReuseStrategy,r.targetRouterState,r.currentRouterState,l=>this.events.next(l),this.inputBindingEnabled).activate(this.rootContexts),c.newlyCreatedRoutes?.clear(),s()&&(z_(c.targetRouterState),i=!0,this.currentNavigation.update(l=>(l.abort=zM,l)),this.lastSuccessfulNavigation.set(te(this.currentNavigation)),this.events.next(new Nn(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects))),this.titleStrategy?.updateTitle(c.targetRouterState.snapshot),c.resolve(!0))}),Jt(k_(a.signal).pipe(dt(()=>!i&&o),Ft(()=>{this.cancelNavigationTransition(r,a.signal.reason+"",Xe.Aborted)}))),Ft({complete:()=>{i=!0}}),Jt(this.transitionAbortWithErrorSubject.pipe(Ft(c=>{throw c}))),Ud(()=>{a.abort(),i||this.cancelNavigationTransition(r,"",Xe.SupersededByNewNavigation),this.currentTransition?.id===r.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),Do(c=>{if(i=!0,e_(r),this.destroyed)return r.resolve(!1),Fe;if(c instanceof vr&&(c=Sa(this.urlSerializer,c)),D_(c))this.events.next(new Wt(r.id,this.urlSerializer.serialize(r.extractedUrl),c.message,c.cancellationCode)),Qk(c)?this.events.next(new ii(c.url,c.navigationBehaviorOptions)):r.resolve(!1);else{let l=new fo(r.id,this.urlSerializer.serialize(r.extractedUrl),c,r.targetSnapshot??void 0);try{let d=Ke(this.environmentInjector,()=>this.navigationErrorHandler?.(l));if(d instanceof vr){let{message:u,cancellationCode:g}=Sa(this.urlSerializer,d);this.events.next(new Wt(r.id,this.urlSerializer.serialize(r.extractedUrl),u,g)),this.events.next(new ii(d.redirectTo,d.navigationBehaviorOptions))}else throw this.events.next(l),c}catch(d){this.options.resolveNavigationPromiseOnError?r.resolve(!1):r.reject(d)}}return Fe}))}))}cancelNavigationTransition(t,r,o){e_(t);let i=new Wt(t.id,this.urlSerializer.serialize(t.extractedUrl),r,o);this.events.next(i),t.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let t=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),r=te(this.currentNavigation),o=r?.targetBrowserUrl??r?.extractedUrl;return t.toString()!==o?.toString()&&!r?.extras.skipLocationChange}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})();function $M(e){return e!==Ca}function e_(e){for(let n of e.newlyCreatedRoutes??[])n._localInjector?.destroy(),n._localInjector=void 0;z_(e.targetRouterState)}function z_(e){if(!e)return;let n=t=>{t.value.pending?.set(!1),t.children.forEach(n)};n(e._root)}var $_=new C("");var G_=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:()=>f(GM)})}return e})(),kl=class{shouldDetach(n){return!1}store(n,t){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,t){return n.routeConfig===t.routeConfig}shouldDestroyInjector(n){return!0}},GM=(()=>{class e extends kl{static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})(),Al=(()=>{class e{urlSerializer=f(si);options=f(li,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=f(ao);urlHandlingStrategy=f(Tl);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new Nt;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:t,initialUrl:r,targetBrowserUrl:o}){let i=t!==void 0?this.urlHandlingStrategy.merge(t,r):r,a=o??i;return a instanceof Nt?this.urlSerializer.serialize(a):a}routerUrlState(t){return t?.targetBrowserUrl===void 0||t?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(t.finalUrl)}}commitTransition({targetRouterState:t,finalUrl:r,initialUrl:o}){r&&t?(this.currentUrlTree=r,this.rawUrlTree=this.urlHandlingStrategy.merge(r,o),this.routerState=t):this.rawUrlTree=o}routerState=v_(null,f(Pe));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:()=>f(WM)})}return e})(),WM=(()=>{class e extends Al{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(t){return this.location.subscribe(r=>{r.type==="popstate"&&setTimeout(()=>{t(r.url,r.state,"popstate",{replaceUrl:!0},r.hasUAVisualTransition)})})}handleRouterEvent(t,r){t instanceof uo?this.updateStateMemento():t instanceof Kn?this.commitTransition(r):t instanceof Ma?this.urlUpdateStrategy==="eager"&&(r.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(r),r)):t instanceof oi?(this.commitTransition(r),this.urlUpdateStrategy==="deferred"&&!r.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(r),r)):t instanceof Wt?S_(t)?t.code===Xe.Redirect&&this.routerState===r.targetRouterState&&this.resetInternalState(r):this.restoreHistory(r):t instanceof fo?this.restoreHistory(r,!0):t instanceof Nn&&(this.lastSuccessfulId=t.id,this.currentPageId=this.browserPageId)}setBrowserUrl(t,r){let{extras:o,id:i}=r,{replaceUrl:a,state:s}=o;if(this.location.isCurrentPathEqualTo(t)||a){let c=this.browserPageId,l=v(v({},s),this.generateNgRouterState(i,c,r));this.location.replaceState(t,"",l)}else{let c=v(v({},s),this.generateNgRouterState(i,this.browserPageId+1,r));this.location.go(t,"",c)}}restoreHistory(t,r=!1){if(this.canceledNavigationResolution==="computed"){let o=this.browserPageId,i=this.currentPageId-o;i!==0&&this.lastSuccessfulId!==-1?this.location.historyGo(i):this.getCurrentUrlTree()===t.finalUrl&&i===0&&(this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(r&&this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:t}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,t??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(t,r,o){return this.canceledNavigationResolution==="computed"?v({navigationId:t,\u0275routerPageId:r},this.routerUrlState(o)):v({navigationId:t},this.routerUrlState(o))}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})();function up(e,n){e.events.pipe(dt(t=>t instanceof Nn||t instanceof Wt||t instanceof fo||t instanceof Kn),be(t=>t instanceof Nn||t instanceof Kn?0:(t instanceof Wt?t.code===Xe.Redirect||t.code===Xe.SupersededByNewNavigation:!1)?2:1),dt(t=>t!==2),er(1)).subscribe(()=>{n()})}var He=(()=>{class e{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=f(um);stateManager=f(Al);options=f(li,{optional:!0})||{};pendingTasks=f(lr);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=f(H_);urlSerializer=f(si);location=f(ao);urlHandlingStrategy=f(Tl);injector=f(Pe);_events=new q;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=f(G_);injectorCleanup=f($_,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=f(La,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!f(Ml,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:t=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new Oe;subscribeToNavigationEvents(){let t=this.navigationTransitions.events.subscribe(r=>{try{let o=this.navigationTransitions.currentTransition,i=te(this.navigationTransitions.currentNavigation);if(o!==null&&i!==null){if(this.stateManager.handleRouterEvent(r,i),r instanceof Wt&&r.code!==Xe.Redirect&&r.code!==Xe.SupersededByNewNavigation)this.navigated=!0;else if(r instanceof Nn)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(r instanceof ii){let a=r.navigationBehaviorOptions,s=this.urlHandlingStrategy.merge(r.url,o.currentRawUrl),c=v({scroll:o.extras.scroll,browserUrl:o.extras.browserUrl,info:o.extras.info,skipLocationChange:o.extras.skipLocationChange,replaceUrl:o.extras.replaceUrl||this.urlUpdateStrategy==="eager"||$M(o.source)},a);this.scheduleNavigation(s,Ca,null,c,o.hasUAVisualTransition,{resolve:o.resolve,reject:o.reject,promise:o.promise})}}qk(r)&&this._events.next(r)}catch(o){this.navigationTransitions.transitionAbortWithErrorSubject.next(o)}});this.eventsSubscription.add(t)}resetRootComponentType(t){this.routerState.root.component=t,this.navigationTransitions.rootComponentType=t}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),Ca,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((t,r,o,i,a)=>{this.navigateToSyncWithBrowser(t,o,r,i,a)})}navigateToSyncWithBrowser(t,r,o,i,a){let s=o?.navigationId?o:null,c=o?.\u0275routerUrl??t;if(o?.\u0275routerUrl&&(i=T(v({},i),{browserUrl:t})),o){let d=v({},o);delete d.navigationId,delete d.\u0275routerPageId,delete d.\u0275routerUrl,Object.keys(d).length!==0&&(i.state=d)}let l=this.parseUrl(c);this.scheduleNavigation(l,r,s,i,a).catch(d=>{this.disposed||this.injector.get(Ht)(d)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return te(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(t){this.config=t.map(cp),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(t,r={}){let{relativeTo:o,queryParams:i,fragment:a,queryParamsHandling:s,preserveFragment:c}=r,l=c?this.currentUrlTree.fragment:a,d=null;switch(s??this.options.defaultQueryParamsHandling){case"merge":d=v(v({},this.currentUrlTree.queryParams),i);break;case"preserve":d=this.currentUrlTree.queryParams;break;default:d=i||null}let u;try{let g=o?o.snapshot:this.routerState.snapshot.root;u=h_(g)}catch{(typeof t[0]!="string"||t[0][0]!=="/")&&(t=[]),u=this.currentUrlTree.root}return g_(u,t,d,l??null,this.urlSerializer)}navigateByUrl(t,r={skipLocationChange:!1}){let o=yr(t)?t:this.parseUrl(t),i=this.urlHandlingStrategy.merge(o,this.rawUrlTree);return this.scheduleNavigation(i,Ca,null,r)}navigate(t,r={skipLocationChange:!1}){return qM(t),this.navigateByUrl(this.createUrlTree(t,r),r)}serializeUrl(t){return this.urlSerializer.serialize(t)}parseUrl(t){try{return this.urlSerializer.parse(t)}catch{return this.console.warn(nr(4018,!1)),this.urlSerializer.parse("/")}}isActive(t,r){let o;if(r===!0?o=v({},i_):r===!1?o=v({},ul):o=v(v({},ul),r),yr(t))return Gm(this.currentUrlTree,t,o);let i=this.parseUrl(t);return Gm(this.currentUrlTree,i,o)}scheduleNavigation(t,r,o,i,a,s){if(this.disposed)return Promise.resolve(!1);let c,l,d;s?(c=s.resolve,l=s.reject,d=s.promise):d=new Promise((g,h)=>{c=g,l=h});let u=this.pendingTasks.add();return up(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(u))}),this.navigationTransitions.handleNavigationRequest({source:r,restoredState:o,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:t,extras:i,hasUAVisualTransition:a,resolve:c,reject:l,promise:d,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),d.catch(Promise.reject.bind(Promise))}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})();function qM(e){for(let n=0;n<e.length;n++)if(e[n]==null)throw new I(4008,!1)}var YM=(()=>{class e{router=f(He);stateManager=f(Al);fragment=E("");queryParams=E({});path=E("");serializer=f(si);constructor(){this.updateState(),this.router.events?.subscribe(t=>{t instanceof Nn&&this.updateState()})}updateState(){let{fragment:t,root:r,queryParams:o}=this.stateManager.getCurrentUrlTree();this.fragment.set(t),this.queryParams.set(o),this.path.set(this.serializer.serialize(new Nt(r)))}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})(),it=(()=>{class e{router;route;tabIndexAttribute;renderer;el;locationStrategy;hrefAttributeValue=f(new pr("href"),{optional:!0});reactiveHref=Yc(()=>{if(!this.isAnchorElement)return this.hrefAttributeValue;this.reactiveRouterState.path(),this._preserveFragment()&&this.reactiveRouterState.fragment();let t=r=>r==="preserve"||r==="merge";return(t(this._queryParamsHandling())||t(this.options?.defaultQueryParamsHandling))&&this.reactiveRouterState.queryParams(),this.computeHref(this.createUrlTree())});get href(){return te(this.reactiveHref)}set href(t){this.reactiveHref.set(t)}set target(t){this._target.set(t)}get target(){return te(this._target)}_target=E(void 0);set queryParams(t){this._queryParams.set(t)}get queryParams(){return te(this._queryParams)}_queryParams=E(void 0,{equal:()=>!1});set fragment(t){this._fragment.set(t)}get fragment(){return te(this._fragment)}_fragment=E(void 0);set queryParamsHandling(t){this._queryParamsHandling.set(t)}get queryParamsHandling(){return te(this._queryParamsHandling)}_queryParamsHandling=E(void 0);set state(t){this._state.set(t)}get state(){return te(this._state)}_state=E(void 0,{equal:()=>!1});set info(t){this._info.set(t)}get info(){return te(this._info)}_info=E(void 0,{equal:()=>!1});set relativeTo(t){this._relativeTo.set(t)}get relativeTo(){return te(this._relativeTo)}_relativeTo=E(void 0);set preserveFragment(t){this._preserveFragment.set(t)}get preserveFragment(){return te(this._preserveFragment)}_preserveFragment=E(!1);set skipLocationChange(t){this._skipLocationChange.set(t)}get skipLocationChange(){return te(this._skipLocationChange)}_skipLocationChange=E(!1);set replaceUrl(t){this._replaceUrl.set(t)}get replaceUrl(){return te(this._replaceUrl)}_replaceUrl=E(!1);browserUrl=En(void 0);isAnchorElement;onChanges=new q;applicationErrorHandler=f(Ht);options=f(li,{optional:!0});reactiveRouterState=f(YM);constructor(t,r,o,i,a,s){this.router=t,this.route=r,this.tabIndexAttribute=o,this.renderer=i,this.el=a,this.locationStrategy=s;let c=a.nativeElement.tagName?.toLowerCase();this.isAnchorElement=c==="a"||c==="area"||!!(typeof customElements=="object"&&customElements.get(c)?.observedAttributes?.includes?.("href"))}setTabIndexIfNotOnNativeEl(t){this.tabIndexAttribute!=null||this.isAnchorElement||this.applyAttributeValue("tabindex",t)}ngOnChanges(t){this.onChanges.next(this)}routerLinkInput=E(null);set routerLink(t){t==null?(this.routerLinkInput.set(null),this.setTabIndexIfNotOnNativeEl(null)):(yr(t)?this.routerLinkInput.set(t):this.routerLinkInput.set(Array.isArray(t)?t:[t]),this.setTabIndexIfNotOnNativeEl("0"))}onClick(t,r,o,i,a){let s=this.urlTree;if(s===null||this.isAnchorElement&&(t!==0||r||o||i||a||typeof this.target=="string"&&this.target!="_self"))return!0;let c=this.browserUrl(),l=v({skipLocationChange:this.skipLocationChange,replaceUrl:this.replaceUrl,state:this.state,info:this.info},c!==void 0&&{browserUrl:c});return this.router.navigateByUrl(s,l)?.catch(d=>{this.applicationErrorHandler(d)}),!this.isAnchorElement}ngOnDestroy(){}applyAttributeValue(t,r){let o=this.renderer,i=this.el.nativeElement;r!==null?o.setAttribute(i,t,r):o.removeAttribute(i,t)}createUrlTree(){let t=this.routerLinkInput();return t===null||!this.router.createUrlTree?null:yr(t)?t:this.router.createUrlTree(t,{relativeTo:this._relativeTo()!==void 0?this._relativeTo():this.route,queryParams:this._queryParams(),fragment:this._fragment(),queryParamsHandling:this._queryParamsHandling(),preserveFragment:this._preserveFragment()})}get urlTree(){return te(()=>this.createUrlTree())}computeHref(t){return t!==null&&this.locationStrategy?this.locationStrategy?.prepareExternalUrl(this.router.serializeUrl(t))??"":null}static \u0275fac=function(r){return new(r||e)(Ie(He),Ie(cn),ca("tabindex"),Ie(Qe),Ie(ue),Ie(Dn))};static \u0275dir=re({type:e,selectors:[["","routerLink",""]],hostVars:2,hostBindings:function(r,o){r&1&&w("click",function(a){return o.onClick(a.button,a.ctrlKey,a.shiftKey,a.altKey,a.metaKey)}),r&2&&ge("href",o.reactiveHref(),zf)("target",o._target())},inputs:{target:"target",queryParams:"queryParams",fragment:"fragment",queryParamsHandling:"queryParamsHandling",state:"state",info:"info",relativeTo:"relativeTo",preserveFragment:[2,"preserveFragment","preserveFragment",ie],skipLocationChange:[2,"skipLocationChange","skipLocationChange",ie],replaceUrl:[2,"replaceUrl","replaceUrl",ie],browserUrl:[1,"browserUrl"],routerLink:"routerLink"},features:[yt]})}return e})();var QM=new C("");function fp(e,...n){return ir([{provide:La,multi:!0,useValue:e},{provide:cn,useFactory:XM},{provide:$c,multi:!0,useFactory:eN},n.map(t=>t.\u0275providers)])}function XM(){return f(He).routerState.root}function JM(e,n){return{\u0275kind:e,\u0275providers:n}}function eN(){let e=f(xe);return n=>{let t=e.get(Wn);if(n!==t.components[0])return;let r=e.get(He),o=e.get(tN);e.get(nN)===1&&r.initialNavigation(),e.get(rN,null,{optional:!0})?.setUpPreloading(),e.get(QM,null,{optional:!0})?.init(),r.resetRootComponentType(t.componentTypes[0]),o.closed||(o.next(),o.complete(),o.unsubscribe())}}var tN=new C("",{factory:()=>new q}),nN=new C("",{factory:()=>1});var rN=new C("");function mp(){return JM(6,[{provide:Dn,useClass:Dm}])}function Va(e){return e.buttons===0||e.detail===0}function ja(e){let n=e.touches&&e.touches[0]||e.changedTouches&&e.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var pp;function W_(){if(pp==null){let e=typeof document<"u"?document.head:null;pp=!!(e&&(e.createShadowRoot||e.attachShadow))}return pp}function hp(e){if(W_()){let n=e.getRootNode?e.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function ln(e){if(e.composedPath)try{return e.composedPath()[0]}catch{}return e.target}var gp;try{gp=typeof Intl<"u"&&Intl.v8BreakIterator}catch{gp=!1}var at=(()=>{class e{_platformId=f(qr);isBrowser=this._platformId?jv(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||gp)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})();var Ba;function q_(){if(Ba==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>Ba=!0}))}finally{Ba=Ba||!1}return Ba}function di(e){return q_()?e:!!e.capture}function dn(e){return e instanceof ue?e.nativeElement:e}var K_=new C("cdk-input-modality-detector-options"),Z_={ignoreKeys:[18,17,224,91,16]},Y_=650,bp={passive:!0,capture:!0},Q_=(()=>{class e{_platform=f(at);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new ze(null);_options;_lastTouchMs=0;_onKeydown=t=>{this._options?.ignoreKeys?.some(r=>r===t.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=ln(t))};_onMousedown=t=>{Date.now()-this._lastTouchMs<Y_||(this._modality.next(Va(t)?"keyboard":"mouse"),this._mostRecentTarget=ln(t))};_onTouchstart=t=>{if(ja(t)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=ln(t)};constructor(){let t=f(Y),r=f(ye),o=f(K_,{optional:!0});if(this._options=v(v({},Z_),o),this.modalityDetected=this._modality.pipe(Gd(1)),this.modalityChanged=this.modalityDetected.pipe(Bd()),this._platform.isBrowser){let i=f(It).createRenderer(null,null);this._listenerCleanups=t.runOutsideAngular(()=>[i.listen(r,"keydown",this._onKeydown,bp),i.listen(r,"mousedown",this._onMousedown,bp),i.listen(r,"touchstart",this._onTouchstart,bp)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(t=>t())}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})(),Ua=(function(e){return e[e.IMMEDIATE=0]="IMMEDIATE",e[e.EVENTUAL=1]="EVENTUAL",e})(Ua||{}),X_=new C("cdk-focus-monitor-default-options"),Rl=di({passive:!0,capture:!0}),Ha=(()=>{class e{_ngZone=f(Y);_platform=f(at);_inputModalityDetector=f(Q_);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=f(ye);_stopInputModalityDetector=new q;constructor(){let t=f(X_,{optional:!0});this._detectionMode=t?.detectionMode||Ua.IMMEDIATE}_rootNodeFocusAndBlurListener=t=>{let r=ln(t);for(let o=r;o;o=o.parentElement)t.type==="focus"?this._onFocus(t,o):this._onBlur(t,o)};monitor(t,r=!1){let o=dn(t);if(!this._platform.isBrowser||o.nodeType!==1)return X();let i=hp(o)||this._document,a=this._elementInfo.get(o);if(a)return r&&(a.checkChildren=!0),a.subject;let s={checkChildren:r,subject:new q,rootNode:i};return this._elementInfo.set(o,s),this._registerGlobalListeners(s),s.subject}stopMonitoring(t){let r=dn(t),o=this._elementInfo.get(r);o&&(o.subject.complete(),this._setClasses(r),this._elementInfo.delete(r),this._removeGlobalListeners(o))}focusVia(t,r,o){let i=dn(t),a=this._document.activeElement;i===a?this._getClosestElementsInfo(i).forEach(([s,c])=>this._originChanged(s,r,c)):(this._setOrigin(r),typeof i.focus=="function"&&i.focus(o))}ngOnDestroy(){this._elementInfo.forEach((t,r)=>this.stopMonitoring(r))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(t){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(t)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:t&&this._isLastInteractionFromInputLabel(t)?"mouse":"program"}_shouldBeAttributedToTouch(t){return this._detectionMode===Ua.EVENTUAL||!!t?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(t,r){t.classList.toggle("cdk-focused",!!r),t.classList.toggle("cdk-touch-focused",r==="touch"),t.classList.toggle("cdk-keyboard-focused",r==="keyboard"),t.classList.toggle("cdk-mouse-focused",r==="mouse"),t.classList.toggle("cdk-program-focused",r==="program")}_setOrigin(t,r=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=t,this._originFromTouchInteraction=t==="touch"&&r,this._detectionMode===Ua.IMMEDIATE){clearTimeout(this._originTimeoutId);let o=this._originFromTouchInteraction?Y_:1;this._originTimeoutId=setTimeout(()=>this._origin=null,o)}})}_onFocus(t,r){let o=this._elementInfo.get(r),i=ln(t);!o||!o.checkChildren&&r!==i||this._originChanged(r,this._getFocusOrigin(i),o)}_onBlur(t,r){let o=this._elementInfo.get(r);!o||o.checkChildren&&t.relatedTarget instanceof Node&&r.contains(t.relatedTarget)||(this._setClasses(r),this._emitOrigin(o,null))}_emitOrigin(t,r){t.subject.observers.length&&this._ngZone.run(()=>t.subject.next(r))}_registerGlobalListeners(t){if(!this._platform.isBrowser)return;let r=t.rootNode,o=this._rootNodeFocusListenerCount.get(r)||0;o||this._ngZone.runOutsideAngular(()=>{r.addEventListener("focus",this._rootNodeFocusAndBlurListener,Rl),r.addEventListener("blur",this._rootNodeFocusAndBlurListener,Rl)}),this._rootNodeFocusListenerCount.set(r,o+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(Jt(this._stopInputModalityDetector)).subscribe(i=>{this._setOrigin(i,!0)}))}_removeGlobalListeners(t){let r=t.rootNode;if(this._rootNodeFocusListenerCount.has(r)){let o=this._rootNodeFocusListenerCount.get(r);o>1?this._rootNodeFocusListenerCount.set(r,o-1):(r.removeEventListener("focus",this._rootNodeFocusAndBlurListener,Rl),r.removeEventListener("blur",this._rootNodeFocusAndBlurListener,Rl),this._rootNodeFocusListenerCount.delete(r))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(t,r,o){this._setClasses(t,r),this._emitOrigin(o,r),this._lastFocusOrigin=r}_getClosestElementsInfo(t){let r=[];return this._elementInfo.forEach((o,i)=>{(i===t||o.checkChildren&&i.contains(t))&&r.push([i,o])}),r}_isLastInteractionFromInputLabel(t){let{_mostRecentTarget:r,mostRecentModality:o}=this._inputModalityDetector;if(o!=="mouse"||!r||r===t||t.nodeName!=="INPUT"&&t.nodeName!=="TEXTAREA"||t.disabled)return!1;let i=t.labels;if(i){for(let a=0;a<i.length;a++)if(i[a].contains(r))return!0}return!1}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})();var Ol=new WeakMap,un=(()=>{class e{_appRef;_injector=f(xe);_environmentInjector=f(Pe);load(t){let r=this._appRef=this._appRef||this._injector.get(Wn),o=Ol.get(r);o||(o={loaders:new Set,refs:[]},Ol.set(r,o),r.onDestroy(()=>{Ol.get(r)?.refs.forEach(i=>i.destroy()),Ol.delete(r)})),o.loaders.has(t)||(o.loaders.add(t),o.refs.push(Av(t,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})();var J_=new Set,mo,yp=(()=>{class e{_platform=f(at);_nonce=f(Kr,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):iN}matchMedia(t){return(this._platform.WEBKIT||this._platform.BLINK)&&oN(t,this._nonce),this._matchMedia(t)}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})();function oN(e,n){if(!J_.has(e))try{mo||(mo=document.createElement("style"),n&&mo.setAttribute("nonce",n),mo.setAttribute("type","text/css"),document.head.appendChild(mo)),mo.sheet&&(mo.sheet.insertRule(`@media ${e.replace(/[{}]/g,"")} {body{ }}`,0),J_.add(e))}catch(t){console.error(t)}}function iN(e){return{matches:e==="all"||e==="",media:e,addListener:()=>{},removeListener:()=>{}}}var aN=(()=>{class e{create(t){return typeof MutationObserver>"u"?null:new MutationObserver(t)}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})();var ex=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=ke({type:e});static \u0275inj=Ee({providers:[aN]})}return e})();function tx(e,...n){return n.length?n.some(t=>e[t]):e.altKey||e.shiftKey||e.ctrlKey||e.metaKey}var nx=new Map,At=class e{_appId=f(Wr);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,t=!1){this._appId!=="ng"&&(n+=this._appId);let r=nx.get(n);return r===void 0?r=0:r++,nx.set(n,r),`${n}${t?e._infix+"-":""}${r}`}static \u0275fac=function(t){return new(t||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})};var ui,rx=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function vp(){if(ui)return ui;if(typeof document!="object"||!document)return ui=new Set(rx),ui;let e=document.createElement("input");return ui=new Set(rx.filter(n=>(e.setAttribute("type",n),e.type===n))),ui}var sN=new C("MATERIAL_ANIMATIONS"),ox=null;function cN(){return f(sN,{optional:!0})?.animationsDisabled||f(qu,{optional:!0})==="NoopAnimations"?"di-disabled":(ox??=f(yp).matchMedia("(prefers-reduced-motion)").matches,ox?"reduced-motion":"enabled")}function _t(){return cN()!=="enabled"}function fi(e){return e!=null&&`${e}`!="false"}var qt=(function(e){return e[e.FADING_IN=0]="FADING_IN",e[e.VISIBLE=1]="VISIBLE",e[e.FADING_OUT=2]="FADING_OUT",e[e.HIDDEN=3]="HIDDEN",e})(qt||{}),_p=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=qt.HIDDEN;constructor(n,t,r,o=!1){this._renderer=n,this.element=t,this.config=r,this._animationForciblyDisabledThroughCss=o}fadeOut(){this._renderer.fadeOutRipple(this)}},ix=di({passive:!0,capture:!0}),xp=class{_events=new Map;addHandler(n,t,r,o){let i=this._events.get(t);if(i){let a=i.get(r);a?a.add(o):i.set(r,new Set([o]))}else this._events.set(t,new Map([[r,new Set([o])]])),n.runOutsideAngular(()=>{document.addEventListener(t,this._delegateEventHandler,ix)})}removeHandler(n,t,r){let o=this._events.get(n);if(!o)return;let i=o.get(t);i&&(i.delete(r),i.size===0&&o.delete(t),o.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,ix)))}_delegateEventHandler=n=>{let t=ln(n);t&&this._events.get(n.type)?.forEach((r,o)=>{(o===t||o.contains(t))&&r.forEach(i=>i.handleEvent(n))})}},za={enterDuration:225,exitDuration:150},dN=800,ax=di({passive:!0,capture:!0}),sx=["mousedown","touchstart"],cx=["mouseup","mouseleave","touchend","touchcancel"],uN=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275cmp=Q({type:e,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(r,o){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--%NS%mat-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2})}return e})(),$a=class e{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new xp;constructor(n,t,r,o,i){this._target=n,this._ngZone=t,this._platform=o,o.isBrowser&&(this._containerElement=dn(r)),i&&i.get(un).load(uN)}fadeInRipple(n,t,r={}){let o=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),i=v(v({},za),r.animation);r.centered&&(n=o.left+o.width/2,t=o.top+o.height/2);let a=r.radius||fN(n,t,o),s=n-o.left,c=t-o.top,l=i.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${s-a}px`,d.style.top=`${c-a}px`,d.style.height=`${a*2}px`,d.style.width=`${a*2}px`,r.color!=null&&(d.style.backgroundColor=r.color),d.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(d);let u=window.getComputedStyle(d),g=u.transitionProperty,h=u.transitionDuration,_=g==="none"||h==="0s"||h==="0s, 0s"||o.width===0&&o.height===0,D=new _p(this,d,r,_);d.style.transform="scale3d(1, 1, 1)",D.state=qt.FADING_IN,r.persistent||(this._mostRecentTransientRipple=D);let A=null;return!_&&(l||i.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let j=()=>{A&&(A.fallbackTimer=null),clearTimeout(Je),this._finishRippleTransition(D)},J=()=>this._destroyRipple(D),Je=setTimeout(J,l+100);d.addEventListener("transitionend",j),d.addEventListener("transitioncancel",J),A={onTransitionEnd:j,onTransitionCancel:J,fallbackTimer:Je}}),this._activeRipples.set(D,A),(_||!l)&&this._finishRippleTransition(D),D}fadeOutRipple(n){if(n.state===qt.FADING_OUT||n.state===qt.HIDDEN)return;let t=n.element,r=v(v({},za),n.config.animation);t.style.transitionDuration=`${r.exitDuration}ms`,t.style.opacity="0",n.state=qt.FADING_OUT,(n._animationForciblyDisabledThroughCss||!r.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let t=dn(n);!this._platform.isBrowser||!t||t===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=t,sx.forEach(r=>{e._eventManager.addHandler(this._ngZone,r,t,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{cx.forEach(t=>{this._triggerElement.addEventListener(t,this,ax)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===qt.FADING_IN?this._startFadeOutTransition(n):n.state===qt.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let t=n===this._mostRecentTransientRipple,{persistent:r}=n.config;n.state=qt.VISIBLE,!r&&(!t||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let t=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=qt.HIDDEN,t!==null&&(n.element.removeEventListener("transitionend",t.onTransitionEnd),n.element.removeEventListener("transitioncancel",t.onTransitionCancel),t.fallbackTimer!==null&&clearTimeout(t.fallbackTimer)),n.element.remove()}_onMousedown(n){let t=Va(n),r=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+dN;!this._target.rippleDisabled&&!t&&!r&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!ja(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let t=n.changedTouches;if(t)for(let r=0;r<t.length;r++)this.fadeInRipple(t[r].clientX,t[r].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let t=n.state===qt.VISIBLE||n.config.terminateOnPointerUp&&n.state===qt.FADING_IN;!n.config.persistent&&t&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(sx.forEach(t=>e._eventManager.removeHandler(t,n,this)),this._pointerUpEventsRegistered&&(cx.forEach(t=>n.removeEventListener(t,this,ax)),this._pointerUpEventsRegistered=!1))}};function fN(e,n,t){let r=Math.max(Math.abs(e-t.left),Math.abs(e-t.right)),o=Math.max(Math.abs(n-t.top),Math.abs(n-t.bottom));return Math.sqrt(r*r+o*o)}var Cp=new C("mat-ripple-global-options"),Fl=(()=>{class e{_elementRef=f(ue);_animationsDisabled=_t();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(t){t&&this.fadeOutAllNonPersistent(),this._disabled=t,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(t){this._trigger=t,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let t=f(Y),r=f(at),o=f(Cp,{optional:!0}),i=f(xe);this._globalOptions=o||{},this._rippleRenderer=new $a(this,t,this._elementRef,r,i)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:v(v(v({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(t,r=0,o){return typeof t=="number"?this._rippleRenderer.fadeInRipple(t,r,v(v({},this.rippleConfig),o)):this._rippleRenderer.fadeInRipple(0,0,v(v({},this.rippleConfig),t))}static \u0275fac=function(r){return new(r||e)};static \u0275dir=re({type:e,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(r,o){r&2&&ce("mat-ripple-unbounded",o.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return e})();var mN={capture:!0},pN=["focus","mousedown","mouseenter","touchstart"],Sp="mat-ripple-loader-uninitialized",wp="mat-ripple-loader-class-name",lx="mat-ripple-loader-centered",Pl="mat-ripple-loader-disabled",dx=(()=>{class e{_document=f(ye);_animationsDisabled=_t();_globalRippleOptions=f(Cp,{optional:!0});_platform=f(at);_ngZone=f(Y);_injector=f(xe);_eventCleanups;_hosts=new Map;constructor(){let t=f(It).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>pN.map(r=>t.listen(this._document,r,this._onInteraction,mN)))}ngOnDestroy(){let t=this._hosts.keys();for(let r of t)this.destroyRipple(r);this._eventCleanups.forEach(r=>r())}configureRipple(t,r){t.setAttribute(Sp,this._globalRippleOptions?.namespace??""),(r.className||!t.hasAttribute(wp))&&t.setAttribute(wp,r.className||""),r.centered&&t.setAttribute(lx,""),r.disabled&&t.setAttribute(Pl,"")}setDisabled(t,r){let o=this._hosts.get(t);o?(o.target.rippleDisabled=r,!r&&!o.hasSetUpEvents&&(o.hasSetUpEvents=!0,o.renderer.setupTriggerEvents(t))):r?t.setAttribute(Pl,""):t.removeAttribute(Pl)}_onInteraction=t=>{let r=ln(t);if(r instanceof HTMLElement){let o=r.closest(`[${Sp}="${this._globalRippleOptions?.namespace??""}"]`);o&&this._createRipple(o)}};_createRipple(t){if(!this._document||this._hosts.has(t))return;t.querySelector(".mat-ripple")?.remove();let r=this._document.createElement("span");r.classList.add("mat-ripple",t.getAttribute(wp)),t.append(r);let o=this._globalRippleOptions,i=this._animationsDisabled?0:o?.animation?.enterDuration??za.enterDuration,a=this._animationsDisabled?0:o?.animation?.exitDuration??za.exitDuration,s={rippleDisabled:this._animationsDisabled||o?.disabled||t.hasAttribute(Pl),rippleConfig:{centered:t.hasAttribute(lx),terminateOnPointerUp:o?.terminateOnPointerUp,animation:{enterDuration:i,exitDuration:a}}},c=new $a(s,this._ngZone,r,this._platform,this._injector),l=!s.rippleDisabled;l&&c.setupTriggerEvents(t),this._hosts.set(t,{target:s,renderer:c,hasSetUpEvents:l}),t.removeAttribute(Sp)}destroyRipple(t){let r=this._hosts.get(t);r&&(r.renderer._removeTriggerEvents(),this._hosts.delete(t))}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})();var mi=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275cmp=Q({type:e,selectors:[["structural-styles"]],decls:0,vars:0,template:function(r,o){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--%NS%mat-focus-indicator-display, none);
  border-width: var(--%NS%mat-focus-indicator-border-width, 3px);
  border-style: var(--%NS%mat-focus-indicator-border-style, solid);
  border-color: var(--%NS%mat-focus-indicator-border-color, transparent);
  border-radius: var(--%NS%mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --%NS%mat-focus-indicator-display: block;
    --%NS%mat-focus-indicator-fallback-border-style: none;
  }
}
`],encapsulation:2})}return e})();var hN=new C("MAT_BUTTON_CONFIG");function ux(e){return e==null?void 0:Xc(e)}var Ep=(()=>{class e{_elementRef=f(ue);_ngZone=f(Y);_animationsDisabled=_t();_config=f(hN,{optional:!0});_focusMonitor=f(Ha);_cleanupClick;_renderer=f(Qe);_rippleLoader=f(dx);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(t){this._disableRipple=t,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(t){this._disabled=t,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(t){this.tabIndex=t}showProgress=En(!1,{transform:ie});constructor(){f(un).load(mi);let t=this._elementRef.nativeElement;this._isAnchor=t.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(t,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(t="program",r){t?this._focusMonitor.focusVia(this._elementRef.nativeElement,t,r):this._elementRef.nativeElement.focus(r)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",t=>{this.disabled&&(t.preventDefault(),t.stopImmediatePropagation())}))}static \u0275fac=function(r){return new(r||e)};static \u0275dir=re({type:e,hostAttrs:[1,"mat-mdc-button-base"],hostVars:15,hostBindings:function(r,o){r&2&&(ge("disabled",o._getDisabledAttribute())("aria-disabled",o._getAriaDisabled())("tabindex",o._getTabIndex()),ro(o.color?"mat-"+o.color:""),ce("mat-mdc-button-progress-indicator-shown",o.showProgress())("mat-mdc-button-disabled",o.disabled)("mat-mdc-button-disabled-interactive",o.disabledInteractive)("mat-unthemed",!o.color)("_mat-animation-noopable",o._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",ie],disabled:[2,"disabled","disabled",ie],ariaDisabled:[2,"aria-disabled","ariaDisabled",ie],disabledInteractive:[2,"disabledInteractive","disabledInteractive",ie],tabIndex:[2,"tabIndex","tabIndex",ux],_tabindex:[2,"tabindex","_tabindex",ux],showProgress:[1,"showProgress"]}})}return e})(),Ga=(()=>{class e extends Ep{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=(function(){let t=["*",[["","progressIndicator",""]]],r=["*","[progressIndicator]"];function o(i,a){i&1&&(zt(0,"div",1),Ce(1,1),$t())}return Q({type:e,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[kt],ngContentSelectors:r,decls:5,vars:1,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(a,s){a&1&&(vt(t),Mt(0,"span",0),Ce(1),M(2,o,2,0,"div",1),Mt(3,"span",2)(4,"span",3)),a&2&&(y(2),N(s.showProgress()?2:-1))},styles:[`.mat-mdc-icon-button {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  border: none;
  outline: none;
  background-color: transparent;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  z-index: 0;
  overflow: visible;
  border-radius: var(--%NS%mat-icon-button-container-shape, var(--%NS%mat-sys-corner-full, 50%));
  flex-shrink: 0;
  text-align: center;
  width: var(--%NS%mat-icon-button-state-layer-size, 40px);
  height: var(--%NS%mat-icon-button-state-layer-size, 40px);
  padding: calc(calc(var(--%NS%mat-icon-button-state-layer-size, 40px) - var(--%NS%mat-icon-button-icon-size, 24px)) / 2);
  font-size: var(--%NS%mat-icon-button-icon-size, 24px);
  color: var(--%NS%mat-icon-button-icon-color, var(--%NS%mat-sys-on-surface-variant));
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-icon-button .mat-mdc-button-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-icon-button .mdc-button__label,
.mat-mdc-icon-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-icon-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-icon-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-ripple-element {
  background-color: var(--%NS%mat-icon-button-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface-variant) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-icon-button-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-icon-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-icon-button-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-icon-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-icon-button-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-icon-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-icon-button-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-icon-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-icon-button-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-icon-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-icon-button-touch-target-size, 48px);
  display: var(--%NS%mat-icon-button-touch-target-display, block);
  left: 50%;
  width: var(--%NS%mat-icon-button-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-icon-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-icon-button[disabled], .mat-mdc-icon-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-icon-button-disabled-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-icon-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-icon-button img,
.mat-mdc-icon-button svg {
  width: var(--%NS%mat-icon-button-icon-size, 24px);
  height: var(--%NS%mat-icon-button-icon-size, 24px);
  vertical-align: baseline;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__determinate-circle-graphic {
  width: inherit;
  height: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__indeterminate-circle-graphic {
  height: 100%;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple {
  border-radius: var(--%NS%mat-icon-button-container-shape, var(--%NS%mat-sys-corner-full, 50%));
}
.mat-mdc-icon-button[hidden] {
  display: none;
}
.mat-mdc-icon-button.mat-unthemed:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-primary:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-accent:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-warn:not(.mdc-ripple-upgraded):focus::before {
  background: transparent;
  opacity: 1;
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})})()}return e})();var gN=new C("cdk-dir-doc",{providedIn:"root",factory:()=>f(ye)}),bN=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function fx(e){let n=e?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?bN.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var Wa=(()=>{class e{get value(){return this.valueSignal()}valueSignal=E("ltr");change=new de;constructor(){let t=f(gN,{optional:!0});if(t){let r=t.body?t.body.dir:null,o=t.documentElement?t.documentElement.dir:null;this.valueSignal.set(fx(r||o||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})();var Kt=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=ke({type:e});static \u0275inj=Ee({})}return e})();var Ll=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=ke({type:e});static \u0275inj=Ee({imports:[Kt]})}return e})();var mx=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),xt=(()=>{class e extends Ep{get appearance(){return this._appearance}set appearance(t){this.setAppearance(t||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let t=yN(this._elementRef.nativeElement);t&&this.setAppearance(t)}setAppearance(t){if(t===this._appearance)return;let r=this._elementRef.nativeElement.classList,o=this._appearance?mx.get(this._appearance):null,i=mx.get(t);o&&r.remove(...o),r.add(...i),this._appearance=t}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=(function(){let t=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""],["",8,"material-symbols-outlined",3,"iconPositionEnd",""],["",8,"material-symbols-rounded",3,"iconPositionEnd",""],["",8,"material-symbols-sharp",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""],["","iconPositionEnd","",8,"material-symbols-outlined"],["","iconPositionEnd","",8,"material-symbols-rounded"],["","iconPositionEnd","",8,"material-symbols-sharp"]],[["","progressIndicator",""]]],r=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd]), .material-symbols-outlined:not([iconPositionEnd]), .material-symbols-rounded:not([iconPositionEnd]), .material-symbols-sharp:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd], .material-symbols-outlined[iconPositionEnd], .material-symbols-rounded[iconPositionEnd], .material-symbols-sharp[iconPositionEnd]","[progressIndicator]"];function o(i,a){i&1&&(zt(0,"div",2),Ce(1,3),$t())}return Q({type:e,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[kt],ngContentSelectors:r,decls:8,vars:5,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(a,s){a&1&&(vt(t),Mt(0,"span",0),Ce(1),zt(2,"span",1),Ce(3,1),$t(),Ce(4,2),M(5,o,2,0,"div",2),Mt(6,"span",3)(7,"span",4)),a&2&&(ce("mdc-button__ripple",!s._isFab)("mdc-fab__ripple",s._isFab),y(5),N(s.showProgress()?5:-1))},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--%NS%mat-button-text-horizontal-padding, 12px);
  height: var(--%NS%mat-button-text-container-height, 40px);
  font-family: var(--%NS%mat-button-text-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-text-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-text-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-text-label-text-transform);
  font-weight: var(--%NS%mat-button-text-label-text-weight, var(--%NS%mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-text-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--%NS%mat-button-text-label-text-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, .material-symbols-outlined, .material-symbols-rounded,
.material-symbols-sharp, mat-icon, [matButtonIcon]) {
  padding: 0 var(--%NS%mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-offset, -4px);
  margin-left: var(--%NS%mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-offset, -4px);
  margin-left: var(--%NS%mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-text-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-text-state-layer-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-text-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-text-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-text-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-text-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-text-touch-target-size, 48px);
  display: var(--%NS%mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--%NS%mat-button-filled-container-height, 40px);
  font-family: var(--%NS%mat-button-filled-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-filled-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-filled-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-filled-label-text-transform);
  font-weight: var(--%NS%mat-button-filled-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  padding: 0 var(--%NS%mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-filled-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-filled-state-layer-color, var(--%NS%mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-filled-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-filled-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-filled-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-filled-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-filled-touch-target-size, 48px);
  display: var(--%NS%mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--%NS%mat-button-filled-label-text-color, var(--%NS%mat-sys-on-primary));
  background-color: var(--%NS%mat-button-filled-container-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-filled-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-mdc-unelevated-button .mat-mdc-button-progress-indicator-container {
  --%NS%mat-progress-spinner-active-indicator-color: var(--%NS%mat-button-filled-progress-active-indicator-color, var(--%NS%mat-sys-on-primary));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-filled-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--%NS%mat-button-protected-container-elevation-shadow, var(--%NS%mat-sys-level1));
  height: var(--%NS%mat-button-protected-container-height, 40px);
  font-family: var(--%NS%mat-button-protected-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-protected-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-protected-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-protected-label-text-transform);
  font-weight: var(--%NS%mat-button-protected-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  padding: 0 var(--%NS%mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-protected-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-protected-state-layer-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-protected-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-protected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-protected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-protected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-protected-touch-target-size, 48px);
  display: var(--%NS%mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--%NS%mat-button-protected-label-text-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-button-protected-container-color, var(--%NS%mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-protected-container-shape, var(--%NS%mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--%NS%mat-button-protected-hover-container-elevation-shadow, var(--%NS%mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--%NS%mat-button-protected-focus-container-elevation-shadow, var(--%NS%mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--%NS%mat-button-protected-pressed-container-elevation-shadow, var(--%NS%mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-protected-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--%NS%mat-button-protected-disabled-container-elevation-shadow, var(--%NS%mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--%NS%mat-button-outlined-container-height, 40px);
  font-family: var(--%NS%mat-button-outlined-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-outlined-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-outlined-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-outlined-label-text-transform);
  font-weight: var(--%NS%mat-button-outlined-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  border-radius: var(--%NS%mat-button-outlined-container-shape, var(--%NS%mat-sys-corner-full));
  border-width: var(--%NS%mat-button-outlined-outline-width, 1px);
  padding: 0 var(--%NS%mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-outlined-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-outlined-state-layer-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-outlined-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-outlined-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-outlined-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-outlined-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-outlined-touch-target-size, 48px);
  display: var(--%NS%mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--%NS%mat-button-outlined-label-text-color, var(--%NS%mat-sys-primary));
  border-color: var(--%NS%mat-button-outlined-outline-color, var(--%NS%mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  border-color: var(--%NS%mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--%NS%mat-button-tonal-container-height, 40px);
  font-family: var(--%NS%mat-button-tonal-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-tonal-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-tonal-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-tonal-label-text-transform);
  font-weight: var(--%NS%mat-button-tonal-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  padding: 0 var(--%NS%mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--%NS%mat-button-tonal-label-text-color, var(--%NS%mat-sys-on-secondary-container));
  background-color: var(--%NS%mat-button-tonal-container-color, var(--%NS%mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-tonal-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-tonal-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-secondary-container) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-tonal-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-tonal-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-tonal-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-tonal-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-tonal-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-tonal-touch-target-size, 48px);
  display: var(--%NS%mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 3px) * -1);
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon,
.mat-mdc-button-progress-indicator-shown [matButtonIcon],
.mat-mdc-button-progress-indicator-shown .mdc-button__label {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})})()}return e})();function yN(e){return e.hasAttribute("mat-raised-button")?"elevated":e.hasAttribute("mat-stroked-button")?"outlined":e.hasAttribute("mat-flat-button")?"filled":e.hasAttribute("mat-button")?"text":null}var Ct=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=ke({type:e});static \u0275inj=Ee({imports:[Ll,Kt]})}return e})();var _x=(()=>{class e{_renderer;_elementRef;onChange=t=>{};onTouched=()=>{};constructor(t,r){this._renderer=t,this._elementRef=r}setProperty(t,r){this._renderer.setProperty(this._elementRef.nativeElement,t,r)}registerOnTouched(t){this.onTouched=t}registerOnChange(t){this.onChange=t}setDisabledState(t){this.setProperty("disabled",t)}static \u0275fac=function(r){return new(r||e)(Ie(Qe),Ie(ue))};static \u0275dir=re({type:e})}return e})(),vN=(()=>{class e extends _x{static \u0275fac=(()=>{let t;return function(o){return(t||(t=qo(e)))(o||e)}})();static \u0275dir=re({type:e,features:[kt]})}return e})(),Ja=new C("");var _N={provide:Ja,useExisting:bt(()=>xx),multi:!0};function xN(){let e=Gt()?Gt().getUserAgent():"";return/android (\d+)/.test(e.toLowerCase())}var CN=new C(""),xx=(()=>{class e extends _x{_compositionMode;_composing=!1;constructor(t,r,o){super(t,r),this._compositionMode=o,this._compositionMode==null&&(this._compositionMode=!xN())}writeValue(t){let r=t??"";this.setProperty("value",r)}_handleInput(t){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(t)}_compositionStart(){this._composing=!0}_compositionEnd(t){this._composing=!1,this._compositionMode&&this.onChange(t)}static \u0275fac=function(r){return new(r||e)(Ie(Qe),Ie(ue),Ie(CN,8))};static \u0275dir=re({type:e,selectors:[["input","formControlName","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControlName","",3,"ngNoCva",""],["input","formControl","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControl","",3,"ngNoCva",""],["input","ngModel","",3,"type","checkbox",3,"ngNoCva",""],["textarea","ngModel","",3,"ngNoCva",""],["","ngDefaultControl",""]],hostBindings:function(r,o){r&1&&w("input",function(a){return o._handleInput(a.target.value)})("blur",function(){return o.onTouched()})("compositionstart",function(){return o._compositionStart()})("compositionend",function(a){return o._compositionEnd(a.target.value)})},standalone:!1,features:[ft([_N]),kt]})}return e})();function kp(e){return e==null||Mp(e)===0}function Mp(e){return e==null?null:Array.isArray(e)||typeof e=="string"?e.length:e instanceof Set?e.size:null}var es=new C(""),Cx=new C(""),SN=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,Xa=class{static min(n){return wN(n)}static max(n){return EN(n)}static required(n){return Sx(n)}static requiredTrue(n){return DN(n)}static email(n){return IN(n)}static minLength(n){return kN(n)}static maxLength(n){return MN(n)}static pattern(n){return NN(n)}static nullValidator(n){return jl()}static compose(n){return Mx(n)}static composeAsync(n){return Nx(n)}};function wN(e){return n=>{if(n.value==null||e==null)return null;let t=parseFloat(n.value);return!isNaN(t)&&t<e?{min:{min:e,actual:n.value}}:null}}function EN(e){return n=>{if(n.value==null||e==null)return null;let t=parseFloat(n.value);return!isNaN(t)&&t>e?{max:{max:e,actual:n.value}}:null}}function Sx(e){return kp(e.value)?{required:!0}:null}function DN(e){return e.value===!0?null:{required:!0}}function IN(e){return kp(e.value)||SN.test(e.value)?null:{email:!0}}function kN(e){return n=>{let t=n.value?.length??Mp(n.value);return t===null||t===0?null:t<e?{minlength:{requiredLength:e,actualLength:t}}:null}}function MN(e){return n=>{let t=n.value?.length??Mp(n.value);return t!==null&&t>e?{maxlength:{requiredLength:e,actualLength:t}}:null}}function NN(e){if(!e)return jl;let n,t;return typeof e=="string"?(t="",e.charAt(0)!=="^"&&(t+="^"),t+=e,e.charAt(e.length-1)!=="$"&&(t+="$"),n=new RegExp(t)):(t=e.toString(),n=e),r=>{if(kp(r.value))return null;let o=r.value;return n.test(o)?null:{pattern:{requiredPattern:t,actualValue:o}}}}function jl(e){return null}function wx(e){return e!=null}function Ex(e){return Gn(e)?we(e):e}function Dx(e){let n={};return e.forEach(t=>{n=t!=null?v(v({},n),t):n}),Object.keys(n).length===0?null:n}function Ix(e,n){return n.map(t=>t(e))}function TN(e){return!e.validate}function kx(e){return e.map(n=>TN(n)?n:t=>n.validate(t))}function Mx(e){if(!e)return null;let n=e.filter(wx);return n.length==0?null:function(t){return Dx(Ix(t,n))}}function Np(e){return e!=null?Mx(kx(e)):null}function Nx(e){if(!e)return null;let n=e.filter(wx);return n.length==0?null:function(t){let r=Ix(t,n).map(Ex);return Vd(r).pipe(be(Dx))}}function Tp(e){return e!=null?Nx(kx(e)):null}function px(e,n){return e===null?[n]:Array.isArray(e)?[...e,n]:[e,n]}function Tx(e){return e._rawValidators}function Ax(e){return e._rawAsyncValidators}function Dp(e){return e?Array.isArray(e)?e:[e]:[]}function Bl(e,n){return Array.isArray(e)?e.includes(n):e===n}function hx(e,n){let t=Dp(n);return Dp(e).forEach(o=>{Bl(t,o)||t.push(o)}),t}function gx(e,n){return Dp(n).filter(t=>!Bl(e,t))}var Ul=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=Np(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=Tp(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,t){return this.control?this.control.hasError(n,t):!1}getError(n,t){return this.control?this.control.getError(n,t):null}},gi=class extends Ul{name;get formDirective(){return null}get path(){return null}};var qa="VALID",Vl="INVALID",pi="PENDING",Ka="DISABLED",_r=class{},Hl=class extends _r{value;source;constructor(n,t){super(),this.value=n,this.source=t}},Ya=class extends _r{pristine;source;constructor(n,t){super(),this.pristine=n,this.source=t}},Qa=class extends _r{touched;source;constructor(n,t){super(),this.touched=n,this.source=t}},hi=class extends _r{status;source;constructor(n,t){super(),this.status=n,this.source=t}},zl=class extends _r{source;constructor(n){super(),this.source=n}},bi=class extends _r{source;constructor(n){super(),this.source=n}};function Rx(e){return(Zl(e)?e.validators:e)||null}function AN(e){return Array.isArray(e)?Np(e):e||null}function Ox(e,n){return(Zl(n)?n.asyncValidators:e)||null}function RN(e){return Array.isArray(e)?Tp(e):e||null}function Zl(e){return e!=null&&!Array.isArray(e)&&typeof e=="object"}function ON(e,n,t){let r=e.controls;if(!(n?Object.keys(r):r).length)throw new I(1e3,"");if(!Fx(r,t))throw new I(1001,"")}function FN(e,n,t){e._forEachChild((r,o)=>{if(t[o]===void 0)throw new I(-1002,"")})}var PN=Symbol();var $l=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_hasRequired=E(!1);_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,t){this[PN]=!0,this._assignValidators(n),this._assignAsyncValidators(t)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n,this._updateHasRequiredValidator()}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return te(this.statusReactive)}set status(n){te(()=>this.statusReactive.set(n))}_status=$e(()=>this.statusReactive());statusReactive=E(void 0);get valid(){return this.status===qa}get invalid(){return this.status===Vl}get pending(){return this.status===pi}get disabled(){return this.status===Ka}get enabled(){return this.status!==Ka}errors;get pristine(){return te(this.pristineReactive)}set pristine(n){te(()=>this.pristineReactive.set(n))}_pristine=$e(()=>this.pristineReactive());pristineReactive=E(!0);get dirty(){return!this.pristine}get touched(){return te(this.touchedReactive)}set touched(n){te(()=>this.touchedReactive.set(n))}_touched=$e(()=>this.touchedReactive());touchedReactive=E(!1);get untouched(){return!this.touched}_events=new q;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(hx(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(hx(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(gx(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(gx(n,this._rawAsyncValidators))}hasValidator(n){return Bl(this._rawValidators,n)}hasAsyncValidator(n){return Bl(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let t=this.touched===!1;this.touched=!0;let r=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(T(v({},n),{sourceControl:r})),t&&n.emitEvent!==!1&&this._events.next(new Qa(!0,r))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(t=>t.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(t=>t.markAllAsTouched(n))}markAsUntouched(n={}){let t=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let r=n.sourceControl??this;this._forEachChild(o=>{o.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:r})}),n.onlySelf||this._parent?._updateTouched(n,r),t&&n.emitEvent!==!1&&this._events.next(new Qa(!1,r))}markAsDirty(n={}){let t=this.pristine===!0;this.pristine=!1;let r=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(T(v({},n),{sourceControl:r})),t&&n.emitEvent!==!1&&this._events.next(new Ya(!1,r))}markAsPristine(n={}){let t=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let r=n.sourceControl??this;this._forEachChild(o=>{o.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,r),t&&n.emitEvent!==!1&&this._events.next(new Ya(!0,r))}markAsPending(n={}){this.status=pi;let t=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new hi(this.status,t)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(T(v({},n),{sourceControl:t}))}disable(n={}){let t=this._parentMarkedDirty(n.onlySelf);this.status=Ka,this.errors=null,this._forEachChild(o=>{o.disable(T(v({},n),{onlySelf:!0}))}),this._updateValue();let r=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Hl(this.value,r)),this._events.next(new hi(this.status,r)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(T(v({},n),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(o=>o(!0))}enable(n={}){let t=this._parentMarkedDirty(n.onlySelf);this.status=qa,this._forEachChild(r=>{r.enable(T(v({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(T(v({},n),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(r=>r(!1))}_updateAncestors(n,t){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},t),this._parent?._updateTouched({},t))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let r=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===qa||this.status===pi)&&this._runAsyncValidator(r,n.emitEvent)}let t=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Hl(this.value,t)),this._events.next(new hi(this.status,t)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(T(v({},n),{sourceControl:t}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(t=>t._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Ka:qa}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,t){if(this.asyncValidator){this.status=pi,this._hasOwnPendingAsyncValidator={emitEvent:t!==!1,shouldHaveEmitted:n!==!1};let r=Ex(this.asyncValidator(this));this._asyncValidationSubscription=r.subscribe(o=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(o,{emitEvent:t,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,t={}){this.errors=n,this._updateControlsErrors(t.emitEvent!==!1,this,t.shouldHaveEmitted)}get(n){let t=n;return t==null||(Array.isArray(t)||(t=t.split(".")),t.length===0)?null:t.reduce((r,o)=>r&&r._find(o),this)}getError(n,t){let r=t?this.get(t):this;return r?.errors?r.errors[n]:null}hasError(n,t){return!!this.getError(n,t)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,t,r){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||r)&&this._events.next(new hi(this.status,t)),this._parent&&this._parent._updateControlsErrors(n,t,r)}_initObservables(){this.valueChanges=new de,this.statusChanges=new de}_calculateStatus(){return this._allControlsDisabled()?Ka:this.errors?Vl:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(pi)?pi:this._anyControlsHaveStatus(Vl)?Vl:qa}_anyControlsHaveStatus(n){return this._anyControls(t=>t.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,t){let r=!this._anyControlsDirty(),o=this.pristine!==r;this.pristine=r,n.onlySelf||this._parent?._updatePristine(n,t),o&&this._events.next(new Ya(this.pristine,t))}_updateTouched(n={},t){this.touched=this._anyControlsTouched(),this._events.next(new Qa(this.touched,t)),n.onlySelf||this._parent?._updateTouched(n,t)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){Zl(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=AN(this._rawValidators),this._updateHasRequiredValidator()}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=RN(this._rawAsyncValidators)}_updateHasRequiredValidator(){te(()=>this._hasRequired.set(this.hasValidator(Xa.required)))}};function Fx(e,n){return Object.hasOwn(e,n)}function LN(e){return e.tagName==="INPUT"||e.tagName==="SELECT"||e.tagName==="TEXTAREA"}function VN(e,n,t,r){switch(t){case"name":e.setAttribute(n,t,r);break;case"disabled":case"readonly":case"required":r?e.setAttribute(n,t,""):e.removeAttribute(n,t);break;case"max":case"min":case"minLength":case"maxLength":r!==void 0?e.setAttribute(n,t,r.toString()):e.removeAttribute(n,t);break}}var Ip=class{kind;context;control;message;constructor({kind:n,context:t,control:r}){this.kind=n,this.context=t,this.control=r}};var Px=new C("",{factory:()=>jN}),jN="always";function bx(e,n,t=!0){let r=()=>{};n?.valueAccessor?.registerOnChange(r),n?.valueAccessor?.registerOnTouched(r),Wl(e,n),e&&(n._invokeOnDestroyCallbacks(),e._registerOnCollectionChange(()=>{}))}function Gl(e,n){e.forEach(t=>{t.registerOnValidatorChange&&t.registerOnValidatorChange(n)})}function Lx(e,n){let t=Tx(e);n.validator!==null?e.setValidators(px(t,n.validator)):typeof t=="function"&&e.setValidators([t]);let r=Ax(e);n.asyncValidator!==null?e.setAsyncValidators(px(r,n.asyncValidator)):typeof r=="function"&&e.setAsyncValidators([r]);let o=()=>e.updateValueAndValidity();Gl(n._rawValidators,o),Gl(n._rawAsyncValidators,o)}function Wl(e,n){let t=!1;if(e!==null){if(n.validator!==null){let o=Tx(e);if(Array.isArray(o)&&o.length>0){let i=o.filter(a=>a!==n.validator);i.length!==o.length&&(t=!0,e.setValidators(i))}}if(n.asyncValidator!==null){let o=Ax(e);if(Array.isArray(o)&&o.length>0){let i=o.filter(a=>a!==n.asyncValidator);i.length!==o.length&&(t=!0,e.setAsyncValidators(i))}}}let r=()=>{};return Gl(n._rawValidators,r),Gl(n._rawAsyncValidators,r),t}function Vx(e,n){e==null,Lx(e,n)}function BN(e,n){return Wl(e,n)}function UN(e){return Object.getPrototypeOf(e.constructor)===vN}function jx(e,n){e._syncPendingControls(),n.forEach(t=>{let r=t.control;r.updateOn==="submit"&&r._pendingChange&&(t.viewToModelUpdate(r._pendingValue),r._pendingChange=!1)})}function HN(e,n){if(!n)return null;Array.isArray(n);let t,r,o;return n.forEach(i=>{i.constructor===xx?t=i:UN(i)?r=i:o=i}),o||r||t||null}function zN(e,n){let t=e.indexOf(n);t>-1&&e.splice(t,1)}var $N=(()=>{class e{_validator=jl;_onChange;_enabled;ngOnChanges(t){if(this.inputName in t){let r=this.normalizeInput(t[this.inputName].currentValue);this._enabled=this.enabled(r),this._validator=this._enabled?this.createValidator(r):jl,this._onChange?.()}}validate(t){return this._validator(t)}registerOnValidatorChange(t){this._onChange=t}enabled(t){return t!=null}static \u0275fac=function(r){return new(r||e)};static \u0275dir=re({type:e,features:[yt]})}return e})();var GN={provide:es,useExisting:bt(()=>Bx),multi:!0};var Bx=(()=>{class e extends $N{required;inputName="required";normalizeInput=ie;createValidator=t=>Sx;enabled(t){return t}static \u0275fac=(()=>{let t;return function(o){return(t||(t=qo(e)))(o||e)}})();static \u0275dir=re({type:e,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(r,o){r&2&&ge("required",o._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[ft([GN]),kt]})}return e})();var ql=class extends Ul{_parent=null;name=null;valueAccessor=null;isCustomControlBased=!1;userOnReset;resetSubscription;set onReset(n){this.userOnReset=n,this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.control&&(this.resetSubscription=this.control.events.subscribe(t=>{t instanceof bi&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription?.add(this.resetSubscription))}isNativeFormElement=!1;rawValueAccessors;_selectedValueAccessor=null;get selectedValueAccessor(){return this._selectedValueAccessor??=HN(this,this.rawValueAccessors)}parseErrorsValidator=null;renderer;injector;requiredValidatorViaDi;subscription;customControlBindings=null;pendingCustomValue=void 0;hasPendingCustomValue=!1;constructor(n,t,r){super(),this.injector=n,this.renderer=t,this.rawValueAccessors=r,this.injector?.get(tt)?.onDestroy(()=>{this.removeParseErrorsValidator(this.control),this.subscription?.unsubscribe()})}setupCustomControl(){this.subscription?.unsubscribe();let n=this.control,t=this.injector?.get(mt);if(!n||!t)return;let r=t.markForCheck.bind(t);if(this.subscription=new Oe,this.subscription.add(()=>this.discardPendingCustomValue(n)),typeof n.registerOnChange=="function"){let o=(i,a)=>{this.discardPendingCustomValue(n),a&&this.viewToModelUpdate(i),r()};n.registerOnChange(o),this.subscription.add(()=>n._unregisterOnChange(o))}this.subscription.add(n.valueChanges.subscribe(r)),this.subscription.add(n.statusChanges.subscribe(r)),this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.userOnReset&&(this.resetSubscription=n.events.subscribe(o=>{o instanceof bi&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription.add(this.resetSubscription)),this.parseErrorsValidator&&n.addValidators(this.parseErrorsValidator)}ngControlCreate(n){!n.nativeElement.hasAttribute?.("ngNoCva")&&(this.rawValueAccessors&&this.rawValueAccessors.length>0||this.valueAccessor!==null)||!n.customControl||(this.isCustomControlBased=!0,n.listenToCustomControlModel(o=>{let i=this.control;i&&(i._pendingValue=o,i._pendingChange=!0,i._pendingDirty=!0,this.pendingCustomValue=o,this.hasPendingCustomValue=!0,i.updateOn==="change"&&this.updateControlFromCustom(i))}),n.listenToCustomControlOutput("touch",()=>{let o=this.control;o&&(o._pendingTouched=!0,o.updateOn==="blur"&&o._pendingChange&&this.updateControlFromCustom(o),o.updateOn!=="submit"&&o.markAsTouched())}),this.customControlBindings={},this.isNativeFormElement=LN(n.nativeElement),this.requiredValidatorViaDi=this._rawValidators.find(o=>o instanceof Bx))}updateControlFromCustom(n){let t=n._pendingValue;n._pendingChange=!1,this.clearPendingCustomValue(),this.customControlBindings&&(this.customControlBindings.value=t),n._pendingDirty&&n.markAsDirty(),n.setValue(t,{emitModelToViewChange:!1}),this.viewToModelUpdate(n.value)}discardPendingCustomValue(n){this.hasPendingCustomValue&&(this.customControlBindings&&(this.customControlBindings.value=this.pendingCustomValue),this.clearPendingCustomValue(),n._pendingValue=n.value,n._pendingChange=!1,n._pendingDirty=!1)}clearPendingCustomValue(){this.pendingCustomValue=void 0,this.hasPendingCustomValue=!1}ngControlUpdate(n,t){if(!this.isCustomControlBased)return;let r=this.control,o=this.customControlBindings;if(!r||!o)return;this.hasPendingCustomValue&&!r._pendingChange&&(o.value=this.pendingCustomValue,this.clearPendingCustomValue()),Object.is(o.value,r.value)||(this.discardPendingCustomValue(r),o.value=r.value,n.setCustomControlModelInput(r.value)),this.bindControlProperty(n,o,"touched",r.touched),this.bindControlProperty(n,o,"dirty",r.dirty),this.bindControlProperty(n,o,"valid",r.valid),this.bindControlProperty(n,o,"invalid",r.invalid),this.bindControlProperty(n,o,"pending",r.pending),this.bindControlProperty(n,o,"disabled",r.disabled),this.shouldBindRequired&&this.bindControlProperty(n,o,"required",this.isRequired);let i=r.errors;if(o.errors!==i){o.errors=i;let a=this._convertErrors(i);n.setInputOnDirectives("errors",a)}}get isRequired(){return(this.requiredValidatorViaDi?._enabled||this.control?._hasRequired())??!1}get shouldBindRequired(){return!0}bindControlProperty(n,t,r,o){if(t[r]===o)return;t[r]=o;let i=n.setInputOnDirectives(r,o);this.isNativeFormElement&&!i&&(r==="disabled"||r==="required")&&this.renderer&&VN(this.renderer,n.nativeElement,r,o)}_convertErrors(n){if(n===null)return[];let t=this.control;return Object.entries(n).map(([r,o])=>new Ip({context:o,kind:r,control:t}))}setParseErrorSource(n){if(n===void 0)return;let t=null,r=$e(()=>{let o=n();return o.length===0?null:o.reduce((i,a)=>(i[a.kind]=a,i),{})});this.parseErrorsValidator=()=>(t=te(r),t),_n(()=>{let o=r();o!==t&&(t=o,this.control?.updateValueAndValidity({emitEvent:!1}),this.injector?.get(mt)?.markForCheck())},{injector:this.injector})}removeParseErrorsValidator(n){this.parseErrorsValidator&&(n?.removeValidators(this.parseErrorsValidator),n?.updateValueAndValidity({emitEvent:!1}))}};var Kl=class extends $l{constructor(n,t,r){super(Rx(t),Ox(r,t)),this.controls=n,this._initObservables(),this._setUpdateStrategy(t),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,t){let r=this._find(n);return r||(this.controls[n]=t,t.setParent(this),t._registerOnCollectionChange(this._onCollectionChange),t)}addControl(n,t,r={}){this.registerControl(n,t),this.updateValueAndValidity({emitEvent:r.emitEvent}),this._onCollectionChange()}removeControl(n,t={}){let r=this._find(n);r&&r._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:t.emitEvent}),this._onCollectionChange()}setControl(n,t,r={}){let o=this._find(n);o&&o._registerOnCollectionChange(()=>{}),delete this.controls[n],t&&this.registerControl(n,t),this.updateValueAndValidity({emitEvent:r.emitEvent}),this._onCollectionChange()}contains(n){return this._find(n)?.enabled===!0}setValue(n,t={}){te(()=>{FN(this,!0,n),Object.keys(n).forEach(r=>{ON(this,!0,r),this.controls[r].setValue(n[r],{onlySelf:!0,emitEvent:t.emitEvent})}),this.updateValueAndValidity(t)})}patchValue(n,t={}){n!=null&&(Object.keys(n).forEach(r=>{let o=this._find(r);o&&o.patchValue(n[r],{onlySelf:!0,emitEvent:t.emitEvent})}),this.updateValueAndValidity(t))}reset(n={},t={}){this._forEachChild((r,o)=>{r.reset(n?n[o]:null,T(v({},t),{onlySelf:!0}))}),this._updatePristine(t,this),this._updateTouched(t,this),this.updateValueAndValidity(t),t?.emitEvent!==!1&&this._events.next(new bi(this))}getRawValue(){return this._reduceChildren({},(n,t,r)=>(n[r]=t.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(t,r)=>r._syncPendingControls()?!0:t);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(t=>{let r=this.controls[t];r&&n(r,t)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[t,r]of Object.entries(this.controls))if(this.contains(t)&&n(r))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(t,r,o)=>((r.enabled||this.disabled)&&(t[o]=r.value),t))}_reduceChildren(n,t){let r=n;return this._forEachChild((o,i)=>{r=t(r,o,i)}),r}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return Fx(this.controls,n)?this.controls[n]:null}};var WN={provide:gi,useExisting:bt(()=>Ap)},Za=Promise.resolve(),Ap=(()=>{class e extends gi{callSetDisabledState;get submitted(){return te(this.submittedReactive)}_submitted=$e(()=>this.submittedReactive());submittedReactive=E(!1);_directives=new Set;form;ngSubmit=new de;options;constructor(t,r,o){super(),this.callSetDisabledState=o,this.form=new Kl({},Np(t),Tp(r))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(t){Za.then(()=>{let r=this._findContainer(t.path);t.control=r.registerControl(t.name,t.control),t._setupWithForm(this.callSetDisabledState),t.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(t)})}getControl(t){return this.form.get(t.path)}removeControl(t){Za.then(()=>{this._findContainer(t.path)?.removeControl(t.name),this._directives.delete(t)})}addFormGroup(t){Za.then(()=>{let r=this._findContainer(t.path),o=new Kl({});Vx(o,t),r.registerControl(t.name,o),o.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(t){Za.then(()=>{this._findContainer(t.path)?.removeControl?.(t.name)})}getFormGroup(t){return this.form.get(t.path)}updateModel(t,r){Za.then(()=>{this.form.get(t.path).setValue(r)})}setValue(t){this.control.setValue(t)}onSubmit(t){return this.submittedReactive.set(!0),jx(this.form,this._directives),this.ngSubmit.emit(t),this.form._events.next(new zl(this.control)),t?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(t=void 0){this.form.reset(t),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(t){return t.pop(),t.length?this.form.get(t):this.form}static \u0275fac=function(r){return new(r||e)(Ie(es,10),Ie(Cx,10),Ie(Px,8))};static \u0275dir=re({type:e,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(r,o){r&1&&w("submit",function(a){return o.onSubmit(a)})("reset",function(){return o.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[ft([WN]),kt]})}return e})();function yx(e,n){let t=e.indexOf(n);t>-1&&e.splice(t,1)}function vx(e){return typeof e=="object"&&e!==null&&Object.keys(e).length===2&&"value"in e&&"disabled"in e}var qN=class extends $l{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,t,r){super(Rx(t),Ox(r,t)),this._applyFormState(n),this._setUpdateStrategy(t),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Zl(t)&&(t.nonNullable||t.initialValueIsDefault)&&(vx(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,t={}){te(()=>{this.value=this._pendingValue=n,this._onChange.length&&t.emitModelToViewChange!==!1&&this._onChange.forEach(r=>r(this.value,t.emitViewToModelChange!==!1)),this.updateValueAndValidity(t)})}patchValue(n,t={}){this.setValue(n,t)}reset(n=this.defaultValue,t={}){this._applyFormState(n),this.markAsPristine(t),this.markAsUntouched(t),this.setValue(this.value,t),t.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,t?.emitEvent!==!1&&this._events.next(new bi(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){yx(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){yx(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){vx(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var KN=e=>e instanceof qN;var ZN=(()=>{class e extends gi{callSetDisabledState;get submitted(){return te(this._submittedReactive)}set submitted(t){this._submittedReactive.set(t)}_submitted=$e(()=>this._submittedReactive());_submittedReactive=E(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(t,r,o){super(),this.callSetDisabledState=o,this._setValidators(t),this._setAsyncValidators(r)}ngOnChanges(t){this.onChanges(t)}ngOnDestroy(){this.onDestroy()}onChanges(t){if(!this.form)throw new I(1052,!1);Object.hasOwn(t,"form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Wl(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(t){let r=this.form.get(t.path);if(!r)throw new I(-1055,!1);return t._setupWithForm(r,this.callSetDisabledState),r.updateValueAndValidity({emitEvent:!1}),this.directives.push(t),r}getControl(t){return this.form.get(t.path)}removeControl(t){bx(t.control||null,t,!1),zN(this.directives,t)}addFormGroup(t){this._setUpFormContainer(t)}removeFormGroup(t){this._cleanUpFormContainer(t)}getFormGroup(t){return this.form.get(t.path)}getFormArray(t){return this.form.get(t.path)}addFormArray(t){this._setUpFormContainer(t)}removeFormArray(t){this._cleanUpFormContainer(t)}updateModel(t,r){this.form.get(t.path).setValue(r)}onReset(){this.resetForm()}resetForm(t=void 0,r={}){this.form.reset(t,r),this._submittedReactive.set(!1)}onSubmit(t){return this.submitted=!0,jx(this.form,this.directives),this.ngSubmit.emit(t),this.form._events.next(new zl(this.control)),t?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(t=>{let r=t.control,o=this.form.get(t.path);r!==o&&(bx(r||null,t),KN(o)&&t._setupWithForm(o,this.callSetDisabledState))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(t){let r=this.form.get(t.path);Vx(r,t),r.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(t){let r=this.form?.get(t.path);r&&BN(r,t)&&r.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){Lx(this.form,this),this._oldForm&&Wl(this._oldForm,this)}static \u0275fac=function(r){return new(r||e)(Ie(es,10),Ie(Cx,10),Ie(Px,8))};static \u0275dir=re({type:e,features:[kt,yt]})}return e})(),YN={provide:gi,useExisting:bt(()=>Rp)},Rp=(()=>{class e extends ZN{form=null;ngSubmit=new de;get control(){return this.form}static \u0275fac=(()=>{let t;return function(o){return(t||(t=qo(e)))(o||e)}})();static \u0275dir=re({type:e,selectors:[["","formGroup",""]],hostBindings:function(r,o){r&1&&w("submit",function(a){return o.onSubmit(a)})("reset",function(){return o.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[ft([YN]),kt]})}return e})();var Hx=Symbol("FIELD_TREE");var zx=Symbol("IS_ASYNC_VALIDATION_RESOURCE"),Ux=class{reducer;create;brand;[zx];constructor(n,t){this.reducer=n,this.create=t}};function ts(e){return typeof e=="function"&&e[Hx]===!0}var Yl=new C("");var $x=(()=>{class e{labelPosition="after";static \u0275fac=function(r){return new(r||e)};static \u0275cmp=(function(){return Q({type:e,selectors:[["","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(o,i){o&2&&ce("mdc-form-field--align-end",i.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},ngContentSelectors:["*"],decls:1,vars:0,template:function(o,i){o&1&&(vt(),Ce(0))},styles:[`.mat-internal-form-field {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: inline-flex;
  align-items: center;
  vertical-align: middle;
}
.mat-internal-form-field > label, .mat-internal-form-field > .mat-internal-form-field-label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
  order: 0;
}
[dir=rtl] .mat-internal-form-field > label, [dir=rtl] .mat-internal-form-field > .mat-internal-form-field-label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
}

.mdc-form-field--align-end > label, .mdc-form-field--align-end > .mat-internal-form-field-label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
  order: -1;
}
[dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end label, [dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end .mat-internal-form-field-label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
}
`],encapsulation:2})})()}return e})();var Op={color:"accent",clickAction:"check-indeterminate",disabledInteractive:!1},QN=new C("mat-checkbox-default-options",{providedIn:"root",factory:()=>Op}),st=(function(e){return e[e.Init=0]="Init",e[e.Checked=1]="Checked",e[e.Unchecked=2]="Unchecked",e[e.Indeterminate=3]="Indeterminate",e})(st||{}),Fp=class{source;checked},Pp=(()=>{class e{_elementRef=f(ue);_changeDetectorRef=f(mt);_ngZone=f(Y);_animationsDisabled=_t();_options=f(QN,{optional:!0});_injector=f(xe);_signalFormField=null;focus(){this._inputElement.nativeElement.focus()}_createChangeEvent(t){let r=new Fp;return r.source=this,r.checked=t,r}_getAnimationTargetElement(){return this._inputElement?.nativeElement}_animationClasses={uncheckedToChecked:"mdc-checkbox--anim-unchecked-checked",uncheckedToIndeterminate:"mdc-checkbox--anim-unchecked-indeterminate",checkedToUnchecked:"mdc-checkbox--anim-checked-unchecked",checkedToIndeterminate:"mdc-checkbox--anim-checked-indeterminate",indeterminateToChecked:"mdc-checkbox--anim-indeterminate-checked",indeterminateToUnchecked:"mdc-checkbox--anim-indeterminate-unchecked"};ariaLabel="";ariaLabelledby=null;ariaDescribedby;ariaExpanded;ariaControls;ariaOwns;_uniqueId;id;get inputId(){return`${this.id||this._uniqueId}-input`}required=!1;labelPosition="after";name=null;change=new de;indeterminateChange=new de;value;disableRipple=!1;_inputElement;tabIndex;color;disabledInteractive;_onTouched=()=>{};_currentAnimationClass="";_currentCheckState=st.Init;_controlValueAccessorChangeFn=()=>{};_validatorChangeFn=()=>{};constructor(){f(un).load(mi);let t=f(new pr("tabindex"),{optional:!0});this._options=this._options||Op,this.color=this._options.color||Op.color,this.tabIndex=t==null?0:parseInt(t)||0,this.id=this._uniqueId=f(At).getId("mat-mdc-checkbox-"),this.disabledInteractive=this._options?.disabledInteractive??!1}ngOnChanges(t){t.required&&this._validatorChangeFn()}ngOnInit(){this._signalFormField=this._injector.get(Yl,void 0,{optional:!0,self:!0})}ngAfterViewInit(){this._syncIndeterminate(this.indeterminate)}get checked(){return this._checked}set checked(t){t!=this.checked&&(this._checked=t,this._changeDetectorRef.markForCheck())}_checked=!1;get disabled(){return this._disabled}set disabled(t){t!==this.disabled&&(this._disabled=t,this._changeDetectorRef.markForCheck())}_disabled=!1;get indeterminate(){return this._indeterminate()}set indeterminate(t){let r=t!=this._indeterminate();this._indeterminate.set(t),r&&(t?this._transitionCheckState(st.Indeterminate):this._transitionCheckState(this.checked?st.Checked:st.Unchecked),this.indeterminateChange.emit(t)),this._syncIndeterminate(t)}_indeterminate=E(!1);_isRippleDisabled(){return this.disableRipple||this.disabled}_onLabelTextChange(){this._changeDetectorRef.detectChanges()}writeValue(t){this.checked=!!t}registerOnChange(t){this._controlValueAccessorChangeFn=t}registerOnTouched(t){this._onTouched=t}setDisabledState(t){this.disabled=t}validate(t){return this.required&&t.value!==!0&&!this._signalFormField?{required:!0}:null}registerOnValidatorChange(t){this._validatorChangeFn=t}_transitionCheckState(t){let r=this._currentCheckState,o=this._getAnimationTargetElement();if(!(r===t||!o)&&(this._currentAnimationClass&&o.classList.remove(this._currentAnimationClass),this._currentAnimationClass=this._getAnimationClassForCheckStateTransition(r,t),this._currentCheckState=t,this._currentAnimationClass.length>0)){o.classList.add(this._currentAnimationClass);let i=this._currentAnimationClass;this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{o.classList.remove(i)},1e3)})}}_emitChangeEvent(){this._controlValueAccessorChangeFn(this.checked),this.change.emit(this._createChangeEvent(this.checked)),this._inputElement&&(this._inputElement.nativeElement.checked=this.checked)}toggle(){this.checked=!this.checked,this._controlValueAccessorChangeFn(this.checked)}_handleInputClick(){let t=this._options?.clickAction;!this.disabled&&t!=="noop"?(this.indeterminate&&t!=="check"&&Promise.resolve().then(()=>{this._indeterminate.set(!1),this.indeterminateChange.emit(!1)}),this._checked=!this._checked,this._transitionCheckState(this._checked?st.Checked:st.Unchecked),this._emitChangeEvent()):(this.disabled&&this.disabledInteractive||!this.disabled&&t==="noop")&&(this._inputElement.nativeElement.checked=this.checked,this._inputElement.nativeElement.indeterminate=this.indeterminate)}_onInteractionEvent(t){t.stopPropagation()}_onBlur(){Promise.resolve().then(()=>{this._onTouched(),this._changeDetectorRef.markForCheck()})}_getAnimationClassForCheckStateTransition(t,r){if(this._animationsDisabled)return"";switch(t){case st.Init:if(r===st.Checked)return this._animationClasses.uncheckedToChecked;if(r==st.Indeterminate)return this._checked?this._animationClasses.checkedToIndeterminate:this._animationClasses.uncheckedToIndeterminate;break;case st.Unchecked:return r===st.Checked?this._animationClasses.uncheckedToChecked:this._animationClasses.uncheckedToIndeterminate;case st.Checked:return r===st.Unchecked?this._animationClasses.checkedToUnchecked:this._animationClasses.checkedToIndeterminate;case st.Indeterminate:return r===st.Checked?this._animationClasses.indeterminateToChecked:this._animationClasses.indeterminateToUnchecked}return""}_syncIndeterminate(t){let r=this._inputElement;r&&(r.nativeElement.indeterminate=t)}_onInputClick(){this._handleInputClick()}_preventBubblingFromLabel(t){t.target&&this._inputElement&&t.target!==this._inputElement.nativeElement&&t.stopPropagation()}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=(function(){let t=["input"],r=["*"];return Q({type:e,selectors:[["mat-checkbox"]],viewQuery:function(i,a){if(i&1&&qn(t,5),i&2){let s;Te(s=Ae())&&(a._inputElement=s.first)}},hostAttrs:[1,"mat-mdc-checkbox"],hostVars:16,hostBindings:function(i,a){i&2&&(fr("id",a.id),ge("tabindex",null)("aria-label",null)("aria-labelledby",null),ro(a.color?"mat-"+a.color:"mat-accent"),ce("_mat-animation-noopable",a._animationsDisabled)("mdc-checkbox--disabled",a.disabled)("mat-mdc-checkbox-disabled",a.disabled)("mat-mdc-checkbox-checked",a.checked)("mat-mdc-checkbox-disabled-interactive",a.disabledInteractive))},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],ariaExpanded:[2,"aria-expanded","ariaExpanded",ie],ariaControls:[0,"aria-controls","ariaControls"],ariaOwns:[0,"aria-owns","ariaOwns"],id:"id",required:[2,"required","required",ie],labelPosition:"labelPosition",name:"name",value:"value",disableRipple:[2,"disableRipple","disableRipple",ie],tabIndex:[2,"tabIndex","tabIndex",o=>o==null?void 0:Xc(o)],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",ie],checked:[2,"checked","checked",ie],disabled:[2,"disabled","disabled",ie],indeterminate:[2,"indeterminate","indeterminate",ie]},outputs:{change:"change",indeterminateChange:"indeterminateChange"},exportAs:["matCheckbox"],features:[ft([{provide:Ja,useExisting:bt(()=>e),multi:!0},{provide:es,useExisting:e,multi:!0}]),yt],ngContentSelectors:r,decls:15,vars:23,consts:[["checkbox",""],["input",""],["label",""],["mat-internal-form-field","",3,"click","labelPosition","for"],[1,"mdc-checkbox"],["aria-hidden","true",1,"mat-mdc-checkbox-touch-target"],["type","checkbox",1,"mdc-checkbox__native-control",3,"blur","click","change","checked","indeterminate","disabled","id","required","tabIndex"],["aria-hidden","true",1,"mdc-checkbox__ripple"],["aria-hidden","true",1,"mdc-checkbox__background"],["focusable","false","viewBox","0 0 24 24",1,"mdc-checkbox__checkmark"],["fill","none","d","M1.73,12.91 8.1,19.28 22.79,4.59",1,"mdc-checkbox__checkmark-path"],[1,"mdc-checkbox__mixedmark"],["mat-ripple","","aria-hidden","true",1,"mat-mdc-checkbox-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mat-internal-form-field-label","mdc-label"]],template:function(i,a){if(i&1&&(vt(),p(0,"label",3),w("click",function(c){return a._preventBubblingFromLabel(c)}),p(1,"span",4,0),oe(3,"span",5),p(4,"input",6,1),w("blur",function(){return a._onBlur()})("click",function(){return a._onInputClick()})("change",function(c){return a._onInteractionEvent(c)}),m(),oe(6,"span",7),p(7,"span",8),Lo(),p(8,"svg",9),oe(9,"path",10),m(),sc(),oe(10,"span",11),m(),oe(11,"span",12),m(),p(12,"span",13,2),Ce(14),m()()),i&2){let s=mr(2);R("labelPosition",a.labelPosition)("for",a.inputId),y(4),ce("mdc-checkbox--selected",a.checked),R("checked",a.checked)("indeterminate",a.indeterminate)("disabled",a.disabled&&!a.disabledInteractive)("id",a.inputId)("required",a.required)("tabIndex",a.disabled&&!a.disabledInteractive?-1:a.tabIndex),ge("aria-label",a.ariaLabel||null)("aria-labelledby",a.ariaLabelledby)("aria-describedby",a.ariaDescribedby)("aria-checked",a.indeterminate?"mixed":null)("aria-controls",a.ariaControls)("aria-disabled",a.disabled&&a.disabledInteractive?!0:null)("aria-expanded",a.ariaExpanded)("aria-owns",a.ariaOwns)("name",a.name)("value",a.value),y(7),R("matRippleTrigger",s)("matRippleDisabled",a.disableRipple||a.disabled)("matRippleCentered",!0)}},dependencies:[Fl,$x],styles:[`.mdc-checkbox {
  display: inline-block;
  position: relative;
  flex: 0 0 18px;
  box-sizing: content-box;
  width: 18px;
  height: 18px;
  line-height: 0;
  white-space: nowrap;
  cursor: pointer;
  vertical-align: bottom;
  padding: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  margin: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
}
.mdc-checkbox:hover > .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-unselected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-unselected-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox:hover > .mat-mdc-checkbox-ripple > .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-unselected-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-unselected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-unselected-focus-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-unselected-focus-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-unselected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-unselected-pressed-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-unselected-pressed-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-selected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-selected-hover-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-selected-hover-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-selected-focus-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-selected-focus-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-selected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-selected-pressed-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-selected-pressed-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  background-color: var(--%NS%mat-checkbox-unselected-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--%NS%mat-checkbox-state-layer-size, 40px);
  height: var(--%NS%mat-checkbox-state-layer-size, 40px);
  top: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
  right: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
  left: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
}

.mdc-checkbox--disabled {
  cursor: default;
  pointer-events: none;
}

.mdc-checkbox__background {
  display: inline-flex;
  position: absolute;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  width: 18px;
  height: 18px;
  border: 2px solid currentColor;
  border-radius: 2px;
  background-color: transparent;
  pointer-events: none;
  will-change: background-color, border-color;
  transition: background-color 90ms cubic-bezier(0.4, 0, 0.6, 1), border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
  -webkit-print-color-adjust: exact;
  color-adjust: exact;
  border-color: var(--%NS%mat-checkbox-unselected-icon-color, var(--%NS%mat-sys-on-surface-variant));
  top: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  left: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - 18px) / 2);
}

.mdc-checkbox__native-control:enabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:enabled:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-selected-icon-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-checkbox-selected-icon-color, var(--%NS%mat-sys-primary));
}

.mdc-checkbox--disabled .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--%NS%mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}
@media (forced-colors: active) {
  .mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
  .mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-unselected-hover-icon-color, var(--%NS%mat-sys-on-surface));
  background-color: transparent;
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-selected-hover-icon-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-checkbox-selected-hover-icon-color, var(--%NS%mat-sys-primary));
}

.mdc-checkbox__native-control:focus:focus:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-unselected-focus-icon-color, var(--%NS%mat-sys-on-surface));
}

.mdc-checkbox__native-control:focus:focus:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-selected-focus-icon-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-checkbox-selected-focus-icon-color, var(--%NS%mat-sys-primary));
}

.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
    border-color: GrayText;
  }
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--%NS%mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}

.mdc-checkbox__checkmark {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  width: 100%;
  opacity: 0;
  transition: opacity 180ms cubic-bezier(0.4, 0, 0.6, 1);
  color: var(--%NS%mat-checkbox-selected-checkmark-color, var(--%NS%mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__checkmark {
    color: CanvasText;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
  color: var(--%NS%mat-checkbox-disabled-selected-checkmark-color, var(--%NS%mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
    color: GrayText;
  }
}

.mdc-checkbox__checkmark-path {
  transition: stroke-dashoffset 180ms cubic-bezier(0.4, 0, 0.6, 1);
  stroke: currentColor;
  stroke-width: 3.12px;
  stroke-dashoffset: 29.7833385;
  stroke-dasharray: 29.7833385;
}

.mdc-checkbox__mixedmark {
  width: 100%;
  height: 0;
  transform: scaleX(0) rotate(0deg);
  border-width: 1px;
  border-style: solid;
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  border-color: var(--%NS%mat-checkbox-selected-checkmark-color, var(--%NS%mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__mixedmark {
    margin: 0 1px;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
  border-color: var(--%NS%mat-checkbox-disabled-selected-checkmark-color, var(--%NS%mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
    border-color: GrayText;
  }
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__background,
.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__background,
.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__background,
.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__background {
  animation-duration: 180ms;
  animation-timing-function: linear;
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-unchecked-checked-checkmark-path 180ms linear;
  transition: none;
}

.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-unchecked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-checked-unchecked-checkmark-path 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__checkmark {
  animation: mdc-checkbox-checked-indeterminate-checkmark 90ms linear;
  transition: none;
}
.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-checked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__checkmark {
  animation: mdc-checkbox-indeterminate-checked-checkmark 500ms linear;
  transition: none;
}
.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-checked-mixedmark 500ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-unchecked-mixedmark 300ms linear;
  transition: none;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path {
  stroke-dashoffset: 0;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transition: opacity 180ms cubic-bezier(0, 0, 0.2, 1), transform 180ms cubic-bezier(0, 0, 0.2, 1);
  opacity: 1;
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(-45deg);
}

.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transform: rotate(45deg);
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(0deg);
  opacity: 1;
}

@keyframes mdc-checkbox-unchecked-checked-checkmark-path {
  0%, 50% {
    stroke-dashoffset: 29.7833385;
  }
  50% {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
  }
  100% {
    stroke-dashoffset: 0;
  }
}
@keyframes mdc-checkbox-unchecked-indeterminate-mixedmark {
  0%, 68.2% {
    transform: scaleX(0);
  }
  68.2% {
    animation-timing-function: cubic-bezier(0, 0, 0, 1);
  }
  100% {
    transform: scaleX(1);
  }
}
@keyframes mdc-checkbox-checked-unchecked-checkmark-path {
  from {
    animation-timing-function: cubic-bezier(0.4, 0, 1, 1);
    opacity: 1;
    stroke-dashoffset: 0;
  }
  to {
    opacity: 0;
    stroke-dashoffset: -29.7833385;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-checkmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(45deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-checkmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(45deg);
    opacity: 0;
  }
  to {
    transform: rotate(360deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(-45deg);
    opacity: 0;
  }
  to {
    transform: rotate(0deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(315deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-unchecked-mixedmark {
  0% {
    animation-timing-function: linear;
    transform: scaleX(1);
    opacity: 1;
  }
  32.8%, 100% {
    transform: scaleX(0);
    opacity: 0;
  }
}
.mat-mdc-checkbox {
  display: inline-block;
  position: relative;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-touch-target,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__native-control,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__ripple,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-ripple::before,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-checkbox label {
  cursor: pointer;
}
.mat-mdc-checkbox .mat-internal-form-field {
  color: var(--%NS%mat-checkbox-label-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-checkbox-label-text-font, var(--%NS%mat-sys-body-medium-font));
  line-height: var(--%NS%mat-checkbox-label-text-line-height, var(--%NS%mat-sys-body-medium-line-height));
  font-size: var(--%NS%mat-checkbox-label-text-size, var(--%NS%mat-sys-body-medium-size));
  letter-spacing: var(--%NS%mat-checkbox-label-text-tracking, var(--%NS%mat-sys-body-medium-tracking));
  font-weight: var(--%NS%mat-checkbox-label-text-weight, var(--%NS%mat-sys-body-medium-weight));
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive input {
  cursor: default;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled label {
  cursor: default;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled .mat-internal-form-field-label {
  color: var(--%NS%mat-checkbox-disabled-label-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-checkbox.mat-mdc-checkbox-disabled .mat-internal-form-field-label {
    color: GrayText;
  }
}
.mat-mdc-checkbox .mat-internal-form-field-label:empty {
  display: none;
}
.mat-mdc-checkbox .mdc-checkbox__ripple {
  opacity: 0;
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple,
.mdc-checkbox__ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
}
.mat-mdc-checkbox .mat-mdc-checkbox-ripple:not(:empty),
.mdc-checkbox__ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-mdc-checkbox-ripple .mat-ripple-element {
  opacity: 0.1;
}

.mat-mdc-checkbox-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--%NS%mat-checkbox-touch-target-size, 48px);
  width: var(--%NS%mat-checkbox-touch-target-size, 48px);
  transform: translate(-50%, -50%);
  display: var(--%NS%mat-checkbox-touch-target-display, block);
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple::before {
  border-radius: 50%;
}

.mdc-checkbox__native-control:focus-visible ~ .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2})})()}return e})(),Gx=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=ke({type:e});static \u0275inj=Ee({imports:[Pp,Kt]})}return e})();async function Wx(e,n,t){let r=[],o=(g,h,_,D)=>r.push({id:g,label:h,ok:_,detail:D});o("https","Sichere Verbindung (https)",isSecureContext,isSecureContext?"ja":"nein \u2013 die App muss \xFCber https ge\xF6ffnet werden");let i=matchMedia("(display-mode: standalone)").matches||matchMedia("(display-mode: minimal-ui)").matches;o("installiert","Als App installiert",i,i?"ja, l\xE4uft als eigene App":"nein, l\xE4uft im Browser-Tab");let a=await e.isAvailable();o("passkey","Passkey mit Fingerabdruck m\xF6glich",a,a?"ja":"nein");let s=await e.prfCapability();o("prf-browser","Browser meldet PRF-Unterst\xFCtzung",s,s===null?"Browser verr\xE4t es nicht":s?"ja":"nein"),o("prf-echt","Fingerabdruck entsperrt die Daten (PRF)",n?!!n.passkey:null,n?n.passkey?"ja, eingerichtet und benutzbar":"nein, Entsperren nur per App-Passwort":"noch nicht eingerichtet");let c;try{c=await navigator.storage?.persisted?.()??null}catch{c=null}o("speicher","Dauerhafter Speicher gew\xE4hrt",c,c===null?"unbekannt":c?"ja, Chrome l\xF6scht die Daten nicht von selbst":"nein, Chrome k\xF6nnte bei Platzmangel l\xF6schen");let l="";try{let g=await navigator.storage?.estimate?.();g?.usage!==void 0&&(l=`${Math.round(g.usage/1024)} kB belegt`)}catch{l=""}o("belegt","Speicherplatz",null,l||"unbekannt");let d=!!navigator.serviceWorker?.controller;o("offline","Offline bereit (Service Worker aktiv)",d,d?"ja":"noch nicht \u2013 App einmal schlie\xDFen und neu \xF6ffnen");let u=typeof CompressionStream<"u";return o("gzip","Komprimierung f\xFCr Backups",u,u?"ja":"nein"),o("version","App-Version",null,t),o("browser","Browser",null,navigator.userAgent),r}function qx(e,n){let t=r=>r===null?"\u2013":r?"OK":"NEIN";return["Haushaltsbuch \u2013 Machbarkeitspr\xFCfung v0.1",`Datum: ${new Date().toLocaleString("de-DE")}`,"",...e.map(r=>`${t(r.ok)}  ${r.label}: ${r.detail}`),"","Von Hand gepr\xFCft:",...n.map(r=>`${r.done?"OK":"offen"}  ${r.label}`)].join(`
`)}var St=class extends Error{constructor(t,r){super(r);this.reason=t}reason},yi=class e{async isAvailable(){try{return!!window.PublicKeyCredential&&await PublicKeyCredential.isUserVerifyingPlatformAuthenticatorAvailable()}catch{return!1}}async prfCapability(){let n=window.PublicKeyCredential;if(!n?.getClientCapabilities)return null;try{let t=await n.getClientCapabilities();return"extension:prf"in t?!!t["extension:prf"]:null}catch{return null}}async register(n){if(!await this.isAvailable())throw new St("nicht-verfuegbar","Auf diesem Ger\xE4t sind keine Passkeys mit Fingerabdruck verf\xFCgbar.");let t;try{t=await navigator.credentials.create({publicKey:{rp:{name:"Mein Haushaltsbuch",id:location.hostname},user:{id:crypto.getRandomValues(new Uint8Array(16)),name:"Haushaltsbuch",displayName:"Mein Haushaltsbuch"},challenge:crypto.getRandomValues(new Uint8Array(32)),pubKeyCredParams:[{type:"public-key",alg:-7},{type:"public-key",alg:-257}],authenticatorSelection:{authenticatorAttachment:"platform",residentKey:"required",userVerification:"required"},timeout:12e4,extensions:{prf:{eval:{first:n}}}}})}catch(o){throw this.translate(o)}let r=t.getClientExtensionResults().prf;return{credentialId:new Uint8Array(t.rawId),prf:r?.results?.first?new Uint8Array(r.results.first):null,prfEnabled:!!r?.enabled||!!r?.results?.first}}async evaluate(n,t){let r;try{r=await navigator.credentials.get({publicKey:{challenge:crypto.getRandomValues(new Uint8Array(32)),rpId:location.hostname,allowCredentials:[{type:"public-key",id:n}],userVerification:"required",timeout:12e4,extensions:{prf:{eval:{first:t}}}}})}catch(i){throw this.translate(i)}let o=r.getClientExtensionResults().prf?.results?.first;if(!o)throw new St("kein-prf","Der Passkey liefert keinen Schl\xFCssel (PRF wird nicht unterst\xFCtzt).");return new Uint8Array(o)}translate(n){let t=n?.name;return t==="NotAllowedError"||t==="AbortError"?new St("abgebrochen","Abgebrochen oder Zeit abgelaufen. Bitte nochmal versuchen."):t==="NotSupportedError"||t==="SecurityError"?new St("nicht-verfuegbar","Passkeys werden hier nicht unterst\xFCtzt. Ist die App \xFCber https ge\xF6ffnet?"):new St("fehler",`Unerwarteter Fehler: ${t??String(n)}`)}static \u0275fac=function(t){return new(t||e)};static \u0275prov=G({token:e,factory:e.\u0275fac,providedIn:"root"})};function ht(e){let n=e instanceof Uint8Array?e:new Uint8Array(e),t="";for(let r=0;r<n.length;r+=32768)t+=String.fromCharCode(...n.subarray(r,r+32768));return btoa(t)}function je(e){let n=atob(e),t=new Uint8Array(n.length);for(let r=0;r<n.length;r++)t[r]=n.charCodeAt(r);return t}function Kx(e){return ht(e).replace(/\+/g,"-").replace(/\//g,"_").replace(/=+$/,"")}function Zx(e){let n=e.replace(/-/g,"+").replace(/_/g,"/");return je(n+"=".repeat((4-n.length%4)%4))}var Yx="0123456789ABCDEFGHJKMNPQRSTVWXYZ";function Qx(e){let n=e(30),t="";for(let r=0;r<30;r++)t+=Yx[n[r]&31];return t}function Xx(e){let n=[];for(let t=0;t<e.length;t+=5)n.push(e.slice(t,t+5));return n}function Ql(e){let n=e.toUpperCase().replace(/[\s-]/g,"").replace(/O/g,"0").replace(/[IL]/g,"1");if(n.length!==30)return null;for(let t of n)if(!Yx.includes(t))return null;return n}function Lp(e){return e.toUpperCase().replace(/[\s-]/g,"").replace(/O/g,"0").replace(/[IL]/g,"1")}var Vp=new TextEncoder,JN="haushaltsbuch/v1/passkey-kek",jp="haushaltsbuch/v1/recovery-kek",ns=6e5,fn=12;function vi(e){return crypto.getRandomValues(new Uint8Array(e))}async function Jx(){return crypto.subtle.generateKey({name:"AES-GCM",length:256},!0,["encrypt","decrypt"])}async function e0(e,n,t){let r=await crypto.subtle.importKey("raw",e,"HKDF",!1,["deriveKey"]);return crypto.subtle.deriveKey({name:"HKDF",hash:"SHA-256",salt:n,info:Vp.encode(t)},r,{name:"AES-KW",length:256},!1,["wrapKey","unwrapKey"])}function _i(e,n){return e0(Vp.encode(e),n,jp)}function Bp(e,n){return e0(e,n,JN)}async function rs(e,n,t=ns){let r=await crypto.subtle.importKey("raw",Vp.encode(e.normalize("NFC")),"PBKDF2",!1,["deriveKey"]);return crypto.subtle.deriveKey({name:"PBKDF2",hash:"SHA-256",salt:n,iterations:t},r,{name:"AES-KW",length:256},!1,["wrapKey","unwrapKey"])}async function Xl(e,n){return new Uint8Array(await crypto.subtle.wrapKey("raw",e,n,"AES-KW"))}function xr(e,n,t=!1){return crypto.subtle.unwrapKey("raw",e,n,"AES-KW",{name:"AES-GCM",length:256},t,["encrypt","decrypt"])}var t0=new TextEncoder,eT=new TextDecoder,n0=(e,n)=>t0.encode(`${e}|${n}`);async function Jl(e,n,t,r,o=1){let i=crypto.getRandomValues(new Uint8Array(12)),a=await crypto.subtle.encrypt({name:"AES-GCM",iv:i,additionalData:n0(n,t)},e,t0.encode(JSON.stringify(r)));return{key:t,v:o,iv:ht(i),ct:ht(a)}}async function Up(e,n,t){let r=await crypto.subtle.decrypt({name:"AES-GCM",iv:je(t.iv),additionalData:n0(n,t.key)},e,je(t.ct));return JSON.parse(eT.decode(r))}async function r0(e){let n=new Blob([e]).stream().pipeThrough(new CompressionStream("gzip"));return new Uint8Array(await new Response(n).arrayBuffer())}async function o0(e){let n=new Blob([e]).stream().pipeThrough(new DecompressionStream("gzip"));return new Uint8Array(await new Response(n).arrayBuffer())}var Hp="haushaltsbuch-backup",zp=1,i0=`${Hp}|${zp}`;async function a0(e,n,t,r,o=new Date){let i=crypto.getRandomValues(new Uint8Array(12)),a=await r0(new TextEncoder().encode(JSON.stringify(t))),s=await crypto.subtle.encrypt({name:"AES-GCM",iv:i,additionalData:new TextEncoder().encode(i0)},e,a);return{format:Hp,formatVersion:zp,appVersion:r,createdAt:o.toISOString(),keys:v({recovery:{kdf:"HKDF-SHA-256",info:jp,salt:n.recovery.salt,wrap:"AES-KW",wrappedKey:n.recovery.wrappedKey}},n.password?{password:{kdf:"PBKDF2-HMAC-SHA-256",iterations:n.password.iterations,salt:n.password.salt,wrap:"AES-KW",wrappedKey:n.password.wrappedKey}}:{}),payload:{cipher:"AES-256-GCM",compression:"gzip",encoding:"base64",aad:i0,iv:ht(i),data:ht(s)}}}var Zt=class extends Error{};function s0(e){let n;try{n=JSON.parse(e)}catch{throw new Zt("Die Datei ist kein Haushaltsbuch-Backup (kein g\xFCltiges JSON).")}if(n?.format!==Hp)throw new Zt("Die Datei ist kein Haushaltsbuch-Backup.");if(n.formatVersion>zp)throw new Zt("Das Backup stammt aus einer neueren App-Version. Bitte zuerst die App aktualisieren.");if(!n.keys?.recovery?.wrappedKey||!n.payload?.data)throw new Zt("Das Backup ist unvollst\xE4ndig.");return n}async function c0(e,n,t=!0){try{if("recoveryKey"in n){let i=await _i(n.recoveryKey,je(e.keys.recovery.salt));return await xr(je(e.keys.recovery.wrappedKey),i,t)}let r=e.keys.password;if(!r)throw new Zt("Dieses Backup hat kein Passwort.");let o=await rs(n.password,je(r.salt),r.iterations??ns);return await xr(je(r.wrappedKey),o,t)}catch(r){throw r instanceof Zt?r:new Zt("recoveryKey"in n?"Der Wiederherstellungsschl\xFCssel passt nicht zu diesem Backup.":"Das Passwort passt nicht zu diesem Backup.")}}async function l0(e,n){try{let t=await crypto.subtle.decrypt({name:"AES-GCM",iv:je(e.payload.iv),additionalData:new TextEncoder().encode(e.payload.aad)},n,je(e.payload.data));return JSON.parse(new TextDecoder().decode(await o0(new Uint8Array(t))))}catch{throw new Zt("Das Backup ist besch\xE4digt oder wurde ver\xE4ndert.")}}var Wp=(e,n)=>n.some(t=>e instanceof t),d0,u0;function tT(){return d0||(d0=[IDBIndex,IDBObjectStore,IDBCursor,IDBDatabase,IDBTransaction])}function nT(){return u0||(u0=["advance","continue","continuePrimaryKey"].map(e=>IDBCursor.prototype[e]))}var qp=new WeakMap,$p=new WeakMap,ed=new WeakMap;function rT(e){let n=new Promise((t,r)=>{let o=()=>{e.removeEventListener("success",i),e.removeEventListener("error",a)},i=()=>{t(ho(e.result)),o()},a=()=>{r(e.error),o()};e.addEventListener("success",i),e.addEventListener("error",a)});return ed.set(n,e),n}function oT(e){if(qp.has(e))return;let n=new Promise((t,r)=>{let o=()=>{e.removeEventListener("complete",i),e.removeEventListener("error",a),e.removeEventListener("abort",a)},i=()=>{t(),o()},a=()=>{r(e.error||new DOMException("AbortError","AbortError")),o()};e.addEventListener("complete",i),e.addEventListener("error",a),e.addEventListener("abort",a)});qp.set(e,n)}var Kp={get(e,n,t){if(e instanceof IDBTransaction){if(n==="done")return qp.get(e);if(n==="store")return t.objectStoreNames[1]?void 0:t.objectStore(t.objectStoreNames[0])}return ho(e[n])},set(e,n,t){return e[n]=t,!0},has(e,n){return e instanceof IDBTransaction&&(n==="done"||n==="store")?!0:n in e}};function h0(e){Kp=e(Kp)}function iT(e){return nT().includes(e)?function(...n){return e.apply(Zp(this),n),ho(this.request)}:function(...n){return ho(e.apply(Zp(this),n))}}function aT(e){return typeof e=="function"?iT(e):(e instanceof IDBTransaction&&oT(e),Wp(e,tT())?new Proxy(e,Kp):e)}function ho(e){if(e instanceof IDBRequest)return rT(e);if($p.has(e))return $p.get(e);let n=aT(e);return Object.is(n,e)||($p.set(e,n),ed.set(n,e)),n}var Zp=e=>ed.get(e);function g0(e,n,{blocked:t,upgrade:r,blocking:o,terminated:i}={}){let a=indexedDB.open(e,n),s=ho(a);return r&&a.addEventListener("upgradeneeded",c=>{r(ho(a.result),c.oldVersion,c.newVersion,ho(a.transaction),c)}),t&&a.addEventListener("blocked",c=>t(c.oldVersion,c.newVersion,c)),s.then(c=>{i&&c.addEventListener("close",()=>i()),o&&c.addEventListener("versionchange",l=>o(l.oldVersion,l.newVersion,l))}).catch(()=>{}),s}var sT=["get","getKey","getAll","getAllKeys","count"],cT=["put","add","delete","clear"],Gp={};function f0(e,n){if(!(e instanceof IDBDatabase&&!(n in e)&&typeof n=="string"))return;if(Gp[n])return Gp[n];let t=n.replace(/FromIndex$/,""),r=n!==t,o=cT.includes(t);if(!(t in(r?IDBIndex:IDBObjectStore).prototype)||!(o||sT.includes(t)))return;let i=async function(a,...s){let c=this.transaction(a,o?"readwrite":"readonly"),l=c.store;return r&&(l=l.index(s.shift())),(await Promise.all([l[t](...s),o&&c.done]))[0]};return Gp[n]=i}h0(e=>T(v({},e),{get:(n,t,r)=>f0(n,t)||e.get(n,t,r),has:(n,t)=>!!f0(n,t)||e.has(n,t)}));var lT=["advance","continue","continuePrimaryKey"],m0={},Yp=new WeakMap,b0=new WeakMap,dT={get(e,n){if(!lT.includes(n))return e[n];let t=m0[n];return t||(t=m0[n]=function(...r){Yp.set(this,b0.get(this)[n](...r))}),t}};async function*uT(...e){let n=this;if(n instanceof IDBCursor||(n=await n.openCursor(...e)),!n)return;n=n;let t=new Proxy(n,dT);for(b0.set(t,n),ed.set(t,Zp(n));n;)yield t,n=await(Yp.get(t)||n.continue()),Yp.delete(t)}function p0(e,n){return n===Symbol.asyncIterator&&Wp(e,[IDBIndex,IDBObjectStore,IDBCursor])||n==="iterate"&&Wp(e,[IDBIndex,IDBObjectStore])}h0(e=>T(v({},e),{get(n,t,r){return p0(n,t)?uT:e.get(n,t,r)},has(n,t){return p0(n,t)||e.has(n,t)}}));var y0="haushaltsbuch",fT=1,xi=["testdata"],os=null;function Ci(){return os??=g0(y0,fT,{upgrade(e){e.objectStoreNames.contains("meta")||e.createObjectStore("meta");for(let n of xi)e.objectStoreNames.contains(n)||e.createObjectStore(n,{keyPath:"key"})}}),os}async function v0(){return(await Ci()).get("meta","vault")}async function Qp(e){await(await Ci()).put("meta",e,"vault")}async function _0(e,n,t=!1){let o=(await Ci()).transaction(["meta",...xi],"readwrite");if(t)for(let i of xi)await o.objectStore(i).clear();await o.objectStore("meta").put(e,"vault");for(let{store:i,rec:a}of n)await o.objectStore(i).put(a);await o.done}async function x0(e,n){await(await Ci()).put(e,n)}async function C0(e,n){return(await Ci()).get(e,n)}async function S0(e){return(await Ci()).getAll(e)}async function w0(){os&&(await os).close(),os=null,await new Promise((e,n)=>{let t=indexedDB.deleteDatabase(y0);t.onsuccess=()=>e(),t.onerror=()=>n(t.error),t.onblocked=()=>e()})}var Si="0.1.0",Re=class e{status=E("laden");meta=E(null);unlockedWith=E(null);dataKey=null;passkeys=f(yi);ready=this.load();async load(){let n=await v0()??null;this.meta.set(n),this.status.set(n?"gesperrt":"neu")}async startSetup(){return{dataKey:await Jx(),recoveryKey:Qx(vi)}}async startRestore(n,t){let r=await c0(n,{recoveryKey:t},!0),o=await l0(n,r);return{dataKey:r,recoveryKey:t,content:o,recovery:{salt:n.keys.recovery.salt,wrappedKey:n.keys.recovery.wrappedKey},passwordFromBackup:n.keys.password?{salt:n.keys.password.salt,iterations:n.keys.password.iterations,wrappedKey:n.keys.password.wrappedKey}:void 0}}async addPasskey(n){let t=vi(32),r=vi(16),o=await this.passkeys.register(t),i=o.prf;if(!i){if(!o.prfEnabled)throw new St("kein-prf","Der Passkey wurde angelegt, kann aber keinen Schl\xFCssel liefern (PRF fehlt).");i=await this.passkeys.evaluate(o.credentialId,t)}n.passkey={credentialId:Kx(o.credentialId),prfSalt:ht(t),hkdfSalt:ht(r),wrappedKey:ht(await Xl(n.dataKey,await Bp(i,r))),createdAt:new Date().toISOString()}}async finishSetup(n,t){let r=n.recovery;if(!r){if(!n.recoveryKey)throw new Error("Wiederherstellungsschl\xFCssel fehlt.");let c=vi(16);r={salt:ht(c),wrappedKey:ht(await Xl(n.dataKey,await _i(n.recoveryKey,c)))}}let o=t?await this.wrapPassword(n.dataKey,t):n.passwordFromBackup;if(!n.passkey&&!o)throw new Error("Ohne Fingerabdruck ist ein App-Passwort n\xF6tig.");let i=v(v({format:"haushaltsbuch-vault",formatVersion:1,createdAt:new Date().toISOString(),appVersion:Si,recovery:r},n.passkey?{passkey:n.passkey}:{}),o?{password:o}:{}),a=await xr(je(r.wrappedKey),await this.recoveryKekFromSession(n,r),!1).catch(()=>n.dataKey),s=[];if(n.content){for(let[c,l]of Object.entries(n.content.stores))if(xi.includes(c))for(let d of l)s.push({store:c,rec:await Jl(n.dataKey,c,d.key,d.value)})}else s.push({store:"testdata",rec:await Jl(n.dataKey,"testdata","eintraege",[])});await _0(i,s,!0);try{await navigator.storage?.persist?.()}catch{}this.meta.set(i),this.dataKey=a,n.recoveryKey=null,this.unlockedWith.set(n.passkey?"passkey":"passwort"),this.status.set("entsperrt")}async recoveryKekFromSession(n,t){if(!n.recoveryKey)throw new Error("kein Schl\xFCssel");return _i(n.recoveryKey,je(t.salt))}async wrapPassword(n,t){if(t.length<fn)throw new Error(`Das App-Passwort braucht mindestens ${fn} Zeichen.`);let r=vi(16);return{salt:ht(r),iterations:ns,wrappedKey:ht(await Xl(n,await rs(t,r)))}}async unlockWithPasskey(n=!1){let t=this.requireMeta();if(!t.passkey)throw new St("nicht-verfuegbar","F\xFCr dieses Ger\xE4t ist kein Fingerabdruck eingerichtet.");let r=await this.passkeys.evaluate(Zx(t.passkey.credentialId),je(t.passkey.prfSalt)),o=await xr(je(t.passkey.wrappedKey),await Bp(r,je(t.passkey.hkdfSalt)),n);return n||this.opened(o,"passkey"),o}async unlockWithPassword(n,t=!1){let r=this.requireMeta();if(!r.password)throw new Error("Es ist kein App-Passwort eingerichtet.");let o;try{o=await xr(je(r.password.wrappedKey),await rs(n,je(r.password.salt),r.password.iterations),t)}catch{throw new Error("Das App-Passwort stimmt nicht.")}return t||this.opened(o,"passwort"),o}async unlockWithRecovery(n){let t=this.requireMeta(),r;try{r=await xr(je(t.recovery.wrappedKey),await _i(n,je(t.recovery.salt)),!0)}catch{throw new Error("Der Wiederherstellungsschl\xFCssel passt nicht.")}return this.opened(r,"wiederherstellung"),r}async replacePasskey(n){let t={dataKey:n,recoveryKey:null};await this.addPasskey(t);let r=T(v({},this.requireMeta()),{passkey:t.passkey});await Qp(r),this.meta.set(r)}async setPassword(n,t){let r="passkey"in t?await this.unlockWithPasskey(!0):await this.unlockWithPassword(t.password,!0),o=T(v({},this.requireMeta()),{password:await this.wrapPassword(r,n)});await Qp(o),this.meta.set(o)}opened(n,t){this.dataKey=n,this.unlockedWith.set(t),this.status.set("entsperrt")}lock(){this.dataKey=null,this.unlockedWith.set(null),this.meta()&&this.status.set("gesperrt")}async read(n,t,r){let o=await C0(n,t);return o?Up(this.requireKey(),n,o):r}async write(n,t,r){await x0(n,await Jl(this.requireKey(),n,t,r))}async createBackupFile(){let n=this.requireKey(),t=this.requireMeta(),r={},o=0;for(let c of xi)r[c]=await Promise.all((await S0(c)).map(async l=>({key:l.key,value:await Up(n,c,l)}))),o+=r[c].length;let i=await a0(n,{recovery:t.recovery,password:t.password},{stores:r},Si),a=new Date;return{name:`haushaltsbuch-backup-${`${a.getFullYear()}-${String(a.getMonth()+1).padStart(2,"0")}-${String(a.getDate()).padStart(2,"0")}`}.hhb`,text:JSON.stringify(i,null,1),entries:o}}async deleteEverything(){this.lock(),await w0(),this.meta.set(null),this.status.set("neu")}requireMeta(){let n=this.meta();if(!n)throw new Error("Die App ist noch nicht eingerichtet.");return n}requireKey(){if(!this.dataKey)throw new Error("Die App ist gesperrt.");return this.dataKey}static \u0275fac=function(t){return new(t||e)};static \u0275prov=G({token:e,factory:e.\u0275fac,providedIn:"root"})};var mT=(e,n)=>n.id,pT=(e,n)=>n.label;function hT(e,n){if(e&1&&(p(0,"div",3)(1,"span",11),b(2),m(),p(3,"div")(4,"div",12),b(5),m(),p(6,"div",13),b(7),m()()()),e&2){let t=n.$implicit;y(),ce("ok",t.ok===!0)("no",t.ok===!1),y(),le(t.ok===null?"\u2013":t.ok?"\u2713":"\u2717"),y(3),le(t.label),y(2),le(t.detail)}}function gT(e,n){e&1&&(p(0,"p",1),b(1,"Pr\xFCfe \u2026"),m())}function bT(e,n){if(e&1){let t=fe();p(0,"mat-checkbox",14),w("change",function(o){let i=P(t).$index,a=x();return L(a.toggle(i,o.checked))}),b(1),m()}if(e&2){let t=n.$implicit;R("checked",t.done),y(),le(t.label)}}function yT(e,n){if(e&1&&(p(0,"p",9),b(1),m()),e&2){let t=x();y(),le(t.copied())}}function vT(e,n){if(e&1&&(p(0,"textarea",10),b(1),m()),e&2){let t=x();y(),le(t.fallback())}}var _T=["App \xFCber \u201EApp installieren\u201C auf den Startbildschirm gebracht","Mit Fingerabdruck entsperrt","Schnellzugriff \u201ENeue Buchung\u201C auf den Startbildschirm gezogen und benutzt","App verlassen, nach \xFCber einer Minute zur\xFCck: wieder gesperrt","Im Flugmodus ge\xF6ffnet und eine Testbuchung erfasst","Backup erstellt und auf dem zweiten Handy eingespielt, Testbuchungen sind da"],td=class e{vault=f(Re);passkeys=f(yi);items=E([]);manual=E(_T.map(n=>({label:n,done:!1})));copied=E("");fallback=E("");ngOnInit(){try{let n=JSON.parse(sessionStorage.getItem("hb-manual")??"[]");this.manual.update(t=>t.map((r,o)=>T(v({},r),{done:!!n[o]})))}catch{}this.refresh()}async refresh(){await this.vault.ready,this.items.set(await Wx(this.passkeys,this.vault.meta(),Si))}toggle(n,t){this.manual.update(r=>r.map((o,i)=>i===n?T(v({},o),{done:t}):o));try{sessionStorage.setItem("hb-manual",JSON.stringify(this.manual().map(r=>r.done)))}catch{}}async copy(){let n=qx(this.items(),this.manual());try{await navigator.clipboard.writeText(n),this.copied.set("Kopiert. Jetzt im Chat bei Claude einf\xFCgen (lange tippen, \u201EEinf\xFCgen\u201C)."),this.fallback.set("")}catch{this.copied.set("Kopieren ging nicht automatisch. Bitte den Text unten markieren und kopieren."),this.fallback.set(n)}}static \u0275fac=function(t){return new(t||e)};static \u0275cmp=Q({type:e,selectors:[["app-check-page"]],decls:23,vars:4,consts:[[1,"page"],[1,"muted"],[1,"card"],[1,"row"],[3,"checked"],[1,"actions"],["mat-flat-button","","type","button","data-testid","copy",3,"click"],["mat-stroked-button","","type","button",3,"click"],["mat-button","",3,"routerLink"],["role","status",1,"hint"],["readonly","","aria-label","Ergebnis zum Kopieren",1,"report"],[1,"mark"],[1,"t"],[1,"muted","small"],[3,"change","checked"]],template:function(t,r){t&1&&(p(0,"div",0)(1,"h1"),b(2,"Machbarkeitspr\xFCfung"),m(),p(3,"p",1),b(4," Das pr\xFCft die App selbst. Darunter kommt die Liste f\xFCr die Tests von Hand. Am Ende \u201EErgebnis kopieren\u201C und im Chat bei mir einf\xFCgen. "),m(),p(5,"div",2),Qo(6,hT,8,7,"div",3,mT,!1,gT,2,0,"p",1),m(),p(9,"div",2)(10,"h2"),b(11,"Von Hand pr\xFCfen"),m(),Qo(12,bT,2,2,"mat-checkbox",4,pT),m(),p(14,"div",5)(15,"button",6),w("click",function(){return r.copy()}),b(16," Ergebnis kopieren "),m(),p(17,"button",7),w("click",function(){return r.refresh()}),b(18,"Neu pr\xFCfen"),m(),p(19,"a",8),b(20,"Zur\xFCck"),m()(),M(21,yT,2,1,"p",9),M(22,vT,2,1,"textarea",10),m()),t&2&&(y(6),Xo(r.items()),y(6),Xo(r.manual()),y(7),R("routerLink",r.vault.status()==="entsperrt"?"/start":"/"),y(2),N(r.copied()?21:-1),y(),N(r.fallback()?22:-1))},dependencies:[Ct,xt,Gx,Pp,it],styles:[".row[_ngcontent-%COMP%]{display:grid;grid-template-columns:28px 1fr;gap:8px;padding:6px 0;border-bottom:1px solid var(--%NS%hb-line)}.mark[_ngcontent-%COMP%]{font-weight:800;text-align:center;color:var(--%NS%hb-muted)}.ok[_ngcontent-%COMP%]{color:var(--%NS%hb-primary)}.no[_ngcontent-%COMP%]{color:var(--%NS%hb-bad-ink)}.t[_ngcontent-%COMP%]{font-weight:600}mat-checkbox[_ngcontent-%COMP%]{display:block;margin:4px 0}.report[_ngcontent-%COMP%]{width:100%;min-height:220px;margin-top:8px;font:13px ui-monospace,monospace}"]})};var is=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new q;bulk={select:n=>this._select(n),deselect:n=>this._deselect(n),setSelection:n=>this._setSelection(n)};constructor(n=!1,t,r=!0,o){this._multiple=n,this._emitChanges=r,this.compareWith=o,t&&t.length&&(n?t.forEach(i=>this._markSelected(i)):this._markSelected(t[0]),this._selectedToEmit.length=0)}select(...n){return this._select(n)}deselect(...n){return this._deselect(n)}setSelection(...n){return this._setSelection(n)}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let t=this._hasQueuedChanges();return n&&this._emitChangeEvent(),t}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_select(n){this._verifyValueAssignment(n),n.forEach(r=>this._markSelected(r));let t=this._hasQueuedChanges();return this._emitChangeEvent(),t}_deselect(n){this._verifyValueAssignment(n),n.forEach(r=>this._unmarkSelected(r));let t=this._hasQueuedChanges();return this._emitChangeEvent(),t}_setSelection(n){this._verifyValueAssignment(n);let t=this.selected,r=new Set(n.map(i=>this._getConcreteValue(i)));n.forEach(i=>this._markSelected(i)),t.filter(i=>!r.has(this._getConcreteValue(i,r))).forEach(i=>this._unmarkSelected(i));let o=this._hasQueuedChanges();return this._emitChangeEvent(),o}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,t){if(this.compareWith){t=t??this._selection;for(let r of t)if(this.compareWith(n,r))return r;return n}else return n}};var E0=(()=>{class e{_animationsDisabled=_t();state="unchecked";disabled=!1;appearance="full";static \u0275fac=function(r){return new(r||e)};static \u0275cmp=Q({type:e,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(r,o){r&2&&ce("mat-pseudo-checkbox-indeterminate",o.state==="indeterminate")("mat-pseudo-checkbox-checked",o.state==="checked")("mat-pseudo-checkbox-disabled",o.disabled)("mat-pseudo-checkbox-minimal",o.appearance==="minimal")("mat-pseudo-checkbox-full",o.appearance==="full")("_mat-animation-noopable",o._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(r,o){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--%NS%mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--%NS%mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--%NS%mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--%NS%mat-pseudo-checkbox-full-unselected-icon-color, var(--%NS%mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--%NS%mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--%NS%mat-pseudo-checkbox-full-selected-icon-color, var(--%NS%mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--%NS%mat-pseudo-checkbox-full-selected-checkmark-color, var(--%NS%mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--%NS%mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--%NS%mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--%NS%mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2})}return e})();var D0=new C("MAT_BUTTON_TOGGLE_DEFAULT_OPTIONS",{providedIn:"root",factory:()=>({hideSingleSelectionIndicator:!1,hideMultipleSelectionIndicator:!1,disabledInteractive:!1})}),I0=new C("MatButtonToggleGroup"),IT={provide:Ja,useExisting:bt(()=>Xp),multi:!0},nd=class{source;value;constructor(n,t){this.source=n,this.value=t}},Xp=(()=>{class e{_changeDetector=f(mt);_dir=f(Wa,{optional:!0});_multiple=!1;_disabled=!1;_disabledInteractive=!1;_selectionModel;_rawValue;_controlValueAccessorChangeFn=()=>{};_onTouched=()=>{};_buttonToggles;appearance;get name(){return this._name}set name(t){this._name=t,this._markButtonsForCheck()}_name=f(At).getId("mat-button-toggle-group-");vertical=!1;get value(){let t=this._selectionModel?this._selectionModel.selected:[];return this.multiple?t.map(r=>r.value):t[0]?t[0].value:void 0}set value(t){this._setSelectionByValue(t),this.valueChange.emit(this.value)}valueChange=new de;get selected(){let t=this._selectionModel?this._selectionModel.selected:[];return this.multiple?t:t[0]||null}get multiple(){return this._multiple}set multiple(t){this._multiple=t,this._markButtonsForCheck()}get disabled(){return this._disabled}set disabled(t){this._disabled=t,this._markButtonsForCheck()}get disabledInteractive(){return this._disabledInteractive}set disabledInteractive(t){this._disabledInteractive=t,this._markButtonsForCheck()}get dir(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}change=new de;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(t){this._hideSingleSelectionIndicator=t,this._markButtonsForCheck()}_hideSingleSelectionIndicator;get hideMultipleSelectionIndicator(){return this._hideMultipleSelectionIndicator}set hideMultipleSelectionIndicator(t){this._hideMultipleSelectionIndicator=t,this._markButtonsForCheck()}_hideMultipleSelectionIndicator;constructor(){let t=f(D0,{optional:!0});this.appearance=t&&t.appearance?t.appearance:"standard",this._hideSingleSelectionIndicator=t?.hideSingleSelectionIndicator??!1,this._hideMultipleSelectionIndicator=t?.hideMultipleSelectionIndicator??!1}ngOnInit(){this._selectionModel=new is(this.multiple,void 0,!1)}ngAfterContentInit(){this._selectionModel.select(...this._buttonToggles.filter(t=>t.checked)),this.multiple||this._initializeTabIndex()}writeValue(t){this.value=t,this._changeDetector.markForCheck()}registerOnChange(t){this._controlValueAccessorChangeFn=t}registerOnTouched(t){this._onTouched=t}setDisabledState(t){this.disabled=t}_keydown(t){if(this.multiple||this.disabled||tx(t))return;let o=t.target.id,i=this._buttonToggles.toArray().findIndex(s=>s.buttonId===o),a=null;switch(t.keyCode){case 32:case 13:a=this._buttonToggles.get(i)||null;break;case 38:a=this._getNextButton(i,-1);break;case 37:a=this._getNextButton(i,this.dir==="ltr"?-1:1);break;case 40:a=this._getNextButton(i,1);break;case 39:a=this._getNextButton(i,this.dir==="ltr"?1:-1);break;default:return}a&&(t.preventDefault(),a._onButtonClick(),a.focus())}_emitChangeEvent(t){let r=new nd(t,this.value);this._rawValue=r.value,this._controlValueAccessorChangeFn(r.value),this.change.emit(r)}_syncButtonToggle(t,r,o=!1,i=!1){!this.multiple&&this.selected&&!t.checked&&(this.selected.checked=!1),this._selectionModel?r?this._selectionModel.select(t):this._selectionModel.deselect(t):i=!0,i?Promise.resolve().then(()=>this._updateModelValue(t,o)):this._updateModelValue(t,o)}_isSelected(t){return this._selectionModel&&this._selectionModel.isSelected(t)}_isPrechecked(t){return typeof this._rawValue>"u"?!1:this.multiple&&Array.isArray(this._rawValue)?this._rawValue.some(r=>t.value!=null&&r===t.value):t.value===this._rawValue}_initializeTabIndex(){if(this._buttonToggles.forEach(t=>{t.tabIndex=-1}),this.selected)this.selected.tabIndex=0;else for(let t=0;t<this._buttonToggles.length;t++){let r=this._buttonToggles.get(t);if(!r.disabled){r.tabIndex=0;break}}}_getNextButton(t,r){let o=this._buttonToggles;for(let i=1;i<=o.length;i++){let a=(t+r*i+o.length)%o.length,s=o.get(a);if(s&&!s.disabled)return s}return null}_setSelectionByValue(t){if(this._rawValue=t,!this._buttonToggles)return;let r=this._buttonToggles.toArray();if(this.multiple&&t?(Array.isArray(t),this._clearSelection(),t.forEach(o=>this._selectValue(o,r))):(this._clearSelection(),this._selectValue(t,r)),!this.multiple&&r.every(o=>o.tabIndex===-1)){for(let o of r)if(!o.disabled){o.tabIndex=0;break}}}_clearSelection(){this._selectionModel.clear(),this._buttonToggles.forEach(t=>{t.checked=!1,this.multiple||(t.tabIndex=-1)})}_selectValue(t,r){for(let o of r)if(o.value===t){o.checked=!0,this._selectionModel.select(o),this.multiple||(o.tabIndex=0);break}}_updateModelValue(t,r){r&&this._emitChangeEvent(t),this.valueChange.emit(this.value)}_markButtonsForCheck(){this._buttonToggles?.forEach(t=>t._markForCheck())}static \u0275fac=function(r){return new(r||e)};static \u0275dir=re({type:e,selectors:[["mat-button-toggle-group"]],contentQueries:function(r,o,i){if(r&1&&no(i,rd,5),r&2){let a;Te(a=Ae())&&(o._buttonToggles=a)}},hostAttrs:[1,"mat-button-toggle-group"],hostVars:6,hostBindings:function(r,o){r&1&&w("keydown",function(a){return o._keydown(a)}),r&2&&(ge("role",o.multiple?"group":"radiogroup")("aria-disabled",o.disabled),ce("mat-button-toggle-vertical",o.vertical)("mat-button-toggle-group-appearance-standard",o.appearance==="standard"))},inputs:{appearance:"appearance",name:"name",vertical:[2,"vertical","vertical",ie],value:"value",multiple:[2,"multiple","multiple",ie],disabled:[2,"disabled","disabled",ie],disabledInteractive:[2,"disabledInteractive","disabledInteractive",ie],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",ie],hideMultipleSelectionIndicator:[2,"hideMultipleSelectionIndicator","hideMultipleSelectionIndicator",ie]},outputs:{valueChange:"valueChange",change:"change"},exportAs:["matButtonToggleGroup"],features:[ft([IT,{provide:I0,useExisting:e}])]})}return e})(),rd=(()=>{class e{_changeDetectorRef=f(mt);_elementRef=f(ue);_focusMonitor=f(Ha);_idGenerator=f(At);_animationDisabled=_t();_checked=!1;ariaLabel;ariaLabelledby=null;_buttonElement;buttonToggleGroup;get buttonId(){return`${this.id}-button`}id;name;value;get tabIndex(){return this._tabIndex()}set tabIndex(t){this._tabIndex.set(t)}_tabIndex;disableRipple=!1;get appearance(){return this.buttonToggleGroup?this.buttonToggleGroup.appearance:this._appearance}set appearance(t){this._appearance=t}_appearance;get checked(){return this.buttonToggleGroup?this.buttonToggleGroup._isSelected(this):this._checked}set checked(t){t!==this._checked&&(this._checked=t,this.buttonToggleGroup&&this.buttonToggleGroup._syncButtonToggle(this,this._checked),this._changeDetectorRef.markForCheck())}get disabled(){return this._disabled||this.buttonToggleGroup&&this.buttonToggleGroup.disabled}set disabled(t){this._disabled=t}_disabled=!1;get disabledInteractive(){return this._disabledInteractive||this.buttonToggleGroup!==null&&this.buttonToggleGroup.disabledInteractive}set disabledInteractive(t){this._disabledInteractive=t}_disabledInteractive;change=new de;constructor(){f(un).load(mi);let t=f(I0,{optional:!0}),r=f(new pr("tabindex"),{optional:!0})||"",o=f(D0,{optional:!0});this._tabIndex=E(parseInt(r)||0),this.buttonToggleGroup=t,this._appearance=o&&o.appearance?o.appearance:"standard",this._disabledInteractive=o?.disabledInteractive??!1}ngOnInit(){let t=this.buttonToggleGroup;this.id=this.id||this._idGenerator.getId("mat-button-toggle-"),t&&(t._isPrechecked(this)?this.checked=!0:t._isSelected(this)!==this._checked&&t._syncButtonToggle(this,this._checked))}ngAfterViewInit(){this._animationDisabled||this._elementRef.nativeElement.classList.add("mat-button-toggle-animations-enabled"),this._focusMonitor.monitor(this._elementRef,!0)}ngOnDestroy(){let t=this.buttonToggleGroup;this._focusMonitor.stopMonitoring(this._elementRef),t&&t._isSelected(this)&&t._syncButtonToggle(this,!1,!1,!0)}focus(t){this._buttonElement.nativeElement.focus(t)}_onButtonClick(){if(this.disabled)return;let t=this.isSingleSelector()?!0:!this._checked;if(t!==this._checked&&(this._checked=t,this.buttonToggleGroup&&(this.buttonToggleGroup._syncButtonToggle(this,this._checked,!0),this.buttonToggleGroup._onTouched())),this.isSingleSelector()){let r=this.buttonToggleGroup._buttonToggles.find(o=>o.tabIndex===0);r&&(r.tabIndex=-1),this.tabIndex=0}this.change.emit(new nd(this,this.value))}_markForCheck(){this._changeDetectorRef.markForCheck()}_getButtonName(){return this.isSingleSelector()?this.buttonToggleGroup.name:this.name||null}isSingleSelector(){return this.buttonToggleGroup&&!this.buttonToggleGroup.multiple}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=(function(){let t=["button"],r=["*"];function o(i,a){if(i&1&&(p(0,"div",2),oe(1,"mat-pseudo-checkbox",6),m()),i&2){let s=x();y(),R("disabled",s.disabled)}}return Q({type:e,selectors:[["mat-button-toggle"]],viewQuery:function(a,s){if(a&1&&qn(t,5),a&2){let c;Te(c=Ae())&&(s._buttonElement=c.first)}},hostAttrs:["role","presentation",1,"mat-button-toggle"],hostVars:14,hostBindings:function(a,s){a&1&&w("focus",function(){return s.focus()}),a&2&&(ge("aria-label",null)("aria-labelledby",null)("id",s.id)("name",null),ce("mat-button-toggle-standalone",!s.buttonToggleGroup)("mat-button-toggle-checked",s.checked)("mat-button-toggle-disabled",s.disabled)("mat-button-toggle-disabled-interactive",s.disabledInteractive)("mat-button-toggle-appearance-standard",s.appearance==="standard"))},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],id:"id",name:"name",value:"value",tabIndex:"tabIndex",disableRipple:[2,"disableRipple","disableRipple",ie],appearance:"appearance",checked:[2,"checked","checked",ie],disabled:[2,"disabled","disabled",ie],disabledInteractive:[2,"disabledInteractive","disabledInteractive",ie]},outputs:{change:"change"},exportAs:["matButtonToggle"],ngContentSelectors:r,decls:7,vars:13,consts:[["button",""],["type","button",1,"mat-button-toggle-button","mat-focus-indicator",3,"click","id","disabled"],[1,"mat-button-toggle-checkbox-wrapper"],[1,"mat-button-toggle-label-content"],[1,"mat-button-toggle-focus-overlay"],["matRipple","",1,"mat-button-toggle-ripple",3,"matRippleTrigger","matRippleDisabled"],["state","checked","aria-hidden","true","appearance","minimal",3,"disabled"]],template:function(a,s){if(a&1&&(vt(),p(0,"button",1,0),w("click",function(){return s._onButtonClick()}),M(2,o,2,1,"div",2),p(3,"span",3),Ce(4),m()(),oe(5,"span",4)(6,"span",5)),a&2){let c=mr(1);R("id",s.buttonId)("disabled",s.disabled&&!s.disabledInteractive||null),ge("role",s.isSingleSelector()?"radio":"button")("tabindex",s.disabled&&!s.disabledInteractive?-1:s.tabIndex)("aria-pressed",s.isSingleSelector()?null:s.checked)("aria-checked",s.isSingleSelector()?s.checked:null)("name",s._getButtonName())("aria-label",s.ariaLabel)("aria-labelledby",s.ariaLabelledby)("aria-disabled",s.disabled&&s.disabledInteractive?"true":null),y(2),N(s.buttonToggleGroup&&(!s.buttonToggleGroup.multiple&&!s.buttonToggleGroup.hideSingleSelectionIndicator||s.buttonToggleGroup.multiple&&!s.buttonToggleGroup.hideMultipleSelectionIndicator)?2:-1),y(4),R("matRippleTrigger",c)("matRippleDisabled",s.disableRipple||s.disabled)}},dependencies:[Fl,E0],styles:[`.mat-button-toggle-standalone,
.mat-button-toggle-group {
  position: relative;
  display: inline-flex;
  flex-direction: row;
  white-space: nowrap;
  overflow: hidden;
  -webkit-tap-highlight-color: transparent;
  border-radius: var(--%NS%mat-button-toggle-legacy-shape);
  transform: translateZ(0);
}
.mat-button-toggle-standalone:not([class*=mat-elevation-z]),
.mat-button-toggle-group:not([class*=mat-elevation-z]) {
  box-shadow: 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12);
}
@media (forced-colors: active) {
  .mat-button-toggle-standalone,
  .mat-button-toggle-group {
    outline: solid 1px;
  }
}

.mat-button-toggle-standalone.mat-button-toggle-appearance-standard,
.mat-button-toggle-group-appearance-standard {
  border-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
  border: solid 1px var(--%NS%mat-button-toggle-divider-color, var(--%NS%mat-sys-outline));
}
.mat-button-toggle-standalone.mat-button-toggle-appearance-standard .mat-pseudo-checkbox,
.mat-button-toggle-group-appearance-standard .mat-pseudo-checkbox {
  --%NS%mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--%NS%mat-button-toggle-selected-state-text-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-button-toggle-standalone.mat-button-toggle-appearance-standard:not([class*=mat-elevation-z]),
.mat-button-toggle-group-appearance-standard:not([class*=mat-elevation-z]) {
  box-shadow: none;
}
@media (forced-colors: active) {
  .mat-button-toggle-standalone.mat-button-toggle-appearance-standard,
  .mat-button-toggle-group-appearance-standard {
    outline: 0;
  }
}

.mat-button-toggle-vertical {
  flex-direction: column;
}
.mat-button-toggle-vertical .mat-button-toggle-label-content {
  display: block;
}

.mat-button-toggle {
  white-space: nowrap;
  position: relative;
  color: var(--%NS%mat-button-toggle-legacy-text-color);
  font-family: var(--%NS%mat-button-toggle-legacy-label-text-font);
  font-size: var(--%NS%mat-button-toggle-legacy-label-text-size);
  line-height: var(--%NS%mat-button-toggle-legacy-label-text-line-height);
  font-weight: var(--%NS%mat-button-toggle-legacy-label-text-weight);
  letter-spacing: var(--%NS%mat-button-toggle-legacy-label-text-tracking);
  --%NS%mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--%NS%mat-button-toggle-legacy-selected-state-text-color);
}
.mat-button-toggle.cdk-keyboard-focused .mat-button-toggle-focus-overlay {
  opacity: var(--%NS%mat-button-toggle-legacy-focus-state-layer-opacity);
}
.mat-button-toggle .mat-icon svg {
  vertical-align: top;
}

.mat-button-toggle-checkbox-wrapper {
  display: inline-block;
  justify-content: flex-start;
  align-items: center;
  width: 0;
  height: 18px;
  line-height: 18px;
  overflow: hidden;
  box-sizing: border-box;
  position: absolute;
  top: 50%;
  left: 16px;
  transform: translate3d(0, -50%, 0);
}
[dir=rtl] .mat-button-toggle-checkbox-wrapper {
  left: auto;
  right: 16px;
}
.mat-button-toggle-appearance-standard .mat-button-toggle-checkbox-wrapper {
  left: 12px;
}
[dir=rtl] .mat-button-toggle-appearance-standard .mat-button-toggle-checkbox-wrapper {
  left: auto;
  right: 12px;
}
.mat-button-toggle-checked .mat-button-toggle-checkbox-wrapper {
  width: 18px;
}
.mat-button-toggle-animations-enabled .mat-button-toggle-checkbox-wrapper {
  transition: width 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-button-toggle-vertical .mat-button-toggle-checkbox-wrapper {
  transition: none;
}

.mat-button-toggle-checked {
  color: var(--%NS%mat-button-toggle-legacy-selected-state-text-color);
  background-color: var(--%NS%mat-button-toggle-legacy-selected-state-background-color);
}

.mat-button-toggle-disabled {
  pointer-events: none;
  color: var(--%NS%mat-button-toggle-legacy-disabled-state-text-color);
  background-color: var(--%NS%mat-button-toggle-legacy-disabled-state-background-color);
  --%NS%mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color: var(--%NS%mat-button-toggle-legacy-disabled-state-text-color);
}
.mat-button-toggle-disabled.mat-button-toggle-checked {
  background-color: var(--%NS%mat-button-toggle-legacy-disabled-selected-state-background-color);
}

.mat-button-toggle-disabled-interactive {
  pointer-events: auto;
}

.mat-button-toggle-appearance-standard {
  color: var(--%NS%mat-button-toggle-text-color, var(--%NS%mat-sys-on-surface));
  background-color: var(--%NS%mat-button-toggle-background-color, transparent);
  font-family: var(--%NS%mat-button-toggle-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-toggle-label-text-size, var(--%NS%mat-sys-label-large-size));
  line-height: var(--%NS%mat-button-toggle-label-text-line-height, var(--%NS%mat-sys-label-large-line-height));
  font-weight: var(--%NS%mat-button-toggle-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  letter-spacing: var(--%NS%mat-button-toggle-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
}
.mat-button-toggle-group-appearance-standard .mat-button-toggle-appearance-standard + .mat-button-toggle-appearance-standard {
  border-left: solid 1px var(--%NS%mat-button-toggle-divider-color, var(--%NS%mat-sys-outline));
}
[dir=rtl] .mat-button-toggle-group-appearance-standard .mat-button-toggle-appearance-standard + .mat-button-toggle-appearance-standard {
  border-left: none;
  border-right: solid 1px var(--%NS%mat-button-toggle-divider-color, var(--%NS%mat-sys-outline));
}
.mat-button-toggle-group-appearance-standard.mat-button-toggle-vertical .mat-button-toggle-appearance-standard + .mat-button-toggle-appearance-standard {
  border-left: none;
  border-right: none;
  border-top: solid 1px var(--%NS%mat-button-toggle-divider-color, var(--%NS%mat-sys-outline));
}
.mat-button-toggle-appearance-standard.mat-button-toggle-checked {
  color: var(--%NS%mat-button-toggle-selected-state-text-color, var(--%NS%mat-sys-on-secondary-container));
  background-color: var(--%NS%mat-button-toggle-selected-state-background-color, var(--%NS%mat-sys-secondary-container));
}
.mat-button-toggle-appearance-standard.mat-button-toggle-disabled {
  color: var(--%NS%mat-button-toggle-disabled-state-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-toggle-disabled-state-background-color, transparent);
}
.mat-button-toggle-appearance-standard.mat-button-toggle-disabled .mat-pseudo-checkbox {
  --%NS%mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color: var(--%NS%mat-button-toggle-disabled-selected-state-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-button-toggle-appearance-standard.mat-button-toggle-disabled.mat-button-toggle-checked {
  color: var(--%NS%mat-button-toggle-disabled-selected-state-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-toggle-disabled-selected-state-background-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-button-toggle-appearance-standard .mat-button-toggle-focus-overlay {
  background-color: var(--%NS%mat-button-toggle-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mat-button-toggle-appearance-standard:hover .mat-button-toggle-focus-overlay {
  opacity: var(--%NS%mat-button-toggle-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-button-toggle-appearance-standard.cdk-keyboard-focused .mat-button-toggle-focus-overlay {
  opacity: var(--%NS%mat-button-toggle-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
@media (hover: none) {
  .mat-button-toggle-appearance-standard:hover .mat-button-toggle-focus-overlay {
    display: none;
  }
}

.mat-button-toggle-label-content {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  padding: 0 16px;
  line-height: var(--%NS%mat-button-toggle-legacy-height);
  position: relative;
}
.mat-button-toggle-appearance-standard .mat-button-toggle-label-content {
  padding: 0 12px;
  line-height: var(--%NS%mat-button-toggle-height, 40px);
}

.mat-button-toggle-label-content > * {
  vertical-align: middle;
}

.mat-button-toggle-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
  pointer-events: none;
  opacity: 0;
  background-color: var(--%NS%mat-button-toggle-legacy-state-layer-color);
}

@media (forced-colors: active) {
  .mat-button-toggle-checked .mat-button-toggle-focus-overlay {
    border-bottom: solid 500px;
    opacity: 0.5;
    height: 0;
  }
  .mat-button-toggle-checked:hover .mat-button-toggle-focus-overlay {
    opacity: 0.6;
  }
  .mat-button-toggle-checked.mat-button-toggle-appearance-standard .mat-button-toggle-focus-overlay {
    border-bottom: solid 500px;
  }
}
.mat-button-toggle .mat-button-toggle-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}

.mat-button-toggle-button {
  border: 0;
  background: none;
  color: inherit;
  padding: 0;
  margin: 0;
  font: inherit;
  outline: none;
  width: 100%;
  cursor: pointer;
}
.mat-button-toggle-animations-enabled .mat-button-toggle-button {
  transition: padding 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-button-toggle-vertical .mat-button-toggle-button {
  transition: none;
}
.mat-button-toggle-disabled .mat-button-toggle-button {
  cursor: default;
}
.mat-button-toggle-button::-moz-focus-inner {
  border: 0;
}
.mat-button-toggle-checked .mat-button-toggle-button:has(.mat-button-toggle-checkbox-wrapper) {
  padding-left: 30px;
}
[dir=rtl] .mat-button-toggle-checked .mat-button-toggle-button:has(.mat-button-toggle-checkbox-wrapper) {
  padding-left: 0;
  padding-right: 30px;
}

.mat-button-toggle-standalone.mat-button-toggle-appearance-standard {
  --%NS%mat-focus-indicator-border-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
}

.mat-button-toggle-group-appearance-standard:not(.mat-button-toggle-vertical) .mat-button-toggle:last-of-type .mat-button-toggle-button::before {
  border-top-right-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
  border-bottom-right-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
}
.mat-button-toggle-group-appearance-standard:not(.mat-button-toggle-vertical) .mat-button-toggle:first-of-type .mat-button-toggle-button::before {
  border-top-left-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
  border-bottom-left-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
}

.mat-button-toggle-group-appearance-standard.mat-button-toggle-vertical .mat-button-toggle:last-of-type .mat-button-toggle-button::before {
  border-bottom-right-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
  border-bottom-left-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
}
.mat-button-toggle-group-appearance-standard.mat-button-toggle-vertical .mat-button-toggle:first-of-type .mat-button-toggle-button::before {
  border-top-right-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
  border-top-left-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
}
`],encapsulation:2})})()}return e})(),k0=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=ke({type:e});static \u0275inj=Ee({imports:[Ll,rd,Kt]})}return e})();var Jp=class{_box;_destroyed=new q;_resizeSubject=new q;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(t=>this._resizeSubject.next(t)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new Z(t=>{let r=this._resizeSubject.subscribe(t);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),r.unsubscribe(),this._elementObservables.delete(n)}}).pipe(be(t=>t.filter(r=>r.target===n)),dt(t=>t.length>0),$d({bufferSize:1,refCount:!0}),Jt(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},M0=(()=>{class e{_cleanupErrorListener;_observers=new Map;_ngZone=f(Y);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,t]of this._observers)t.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(t,r){let o=r?.box||"content-box";return this._observers.has(o)||this._observers.set(o,new Jp(o)),this._observers.get(o).observe(t)}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})();var mn=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275dir=re({type:e,selectors:[["mat-label"]]})}return e})(),MT=new C("MatError");var eh=(()=>{class e{align="start";id=f(At).getId("mat-mdc-hint-");static \u0275fac=function(r){return new(r||e)};static \u0275dir=re({type:e,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(r,o){r&2&&(fr("id",o.id),ge("align",null),ce("mat-mdc-form-field-hint-end",o.align==="end"))},inputs:{align:"align",id:"id"}})}return e})(),NT=new C("MatPrefix");var TT=new C("MatSuffix");var P0=new C("FloatingLabelParent"),N0=(()=>{class e{_elementRef=f(ue);get floating(){return this._floating}set floating(t){this._floating=t,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(t){this._monitorResize=t,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=f(M0);_ngZone=f(Y);_parent=f(P0);_resizeSubscription=new Oe;ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return AT(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(r){return new(r||e)};static \u0275dir=re({type:e,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(r,o){r&2&&ce("mdc-floating-label--float-above",o.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return e})();function AT(e){let n=e;if(n.offsetParent!==null)return n.scrollWidth;let t=n.cloneNode(!0);t.style.setProperty("position","absolute"),t.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(t);let r=t.scrollWidth;return t.remove(),r}var T0="mdc-line-ripple--active",od="mdc-line-ripple--deactivating",A0=(()=>{class e{_elementRef=f(ue);_cleanupTransitionEnd;constructor(){let t=f(Y),r=f(Qe);t.runOutsideAngular(()=>{this._cleanupTransitionEnd=r.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let t=this._elementRef.nativeElement.classList;t.remove(od),t.add(T0)}deactivate(){this._elementRef.nativeElement.classList.add(od)}_handleTransitionEnd=t=>{let r=this._elementRef.nativeElement.classList,o=r.contains(od);t.propertyName==="opacity"&&o&&r.remove(T0,od)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(r){return new(r||e)};static \u0275dir=re({type:e,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return e})(),R0=(()=>{class e{_elementRef=f(ue);_ngZone=f(Y);open=!1;_notch;ngAfterViewInit(){let t=this._elementRef.nativeElement,r=t.querySelector(".mdc-floating-label");r?(t.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(r.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>r.style.transitionDuration="")}))):t.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(t){let r=this._notch.nativeElement;!this.open||!t?r.style.width="":r.style.width=`calc(${t}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(t){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${t}px)`)}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=(function(){let t=["notch"];return Q({type:e,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,a){if(i&1&&qn(t,5),i&2){let s;Te(s=Ae())&&(a._notch=s.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,a){i&2&&ce("mdc-notched-outline--notched",a.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},ngContentSelectors:["*"],decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,a){i&1&&(vt(),Mt(0,"div",1),zt(1,"div",2,0),Ce(3),$t(),Mt(4,"div",3))},encapsulation:2})})()}return e})(),th=(()=>{class e{id;ngField=null;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;stateChanges=null;value;static \u0275fac=function(r){return new(r||e)};static \u0275dir=re({type:e})}return e})();var nh=new C("MatFormField"),RT=new C("MAT_FORM_FIELD_DEFAULT_OPTIONS"),O0="fill",OT="auto",F0="fixed",FT="translateY(-50%)",Tn=(()=>{class e{_elementRef=f(ue);_changeDetectorRef=f(mt);_platform=f(at);_idGenerator=f(At);_ngZone=f(Y);_defaults=f(RT,{optional:!0});_currentDirection;_unwrapMaybeSignal(t){return xn(t)?t():t}_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=pa("iconPrefixContainer");_textPrefixContainerSignal=pa("textPrefixContainer");_iconSuffixContainerSignal=pa("iconSuffixContainer");_textSuffixContainerSignal=pa("textSuffixContainer");_prefixSuffixContainers=$e(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(t=>t?.nativeElement).filter(t=>t!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=Mv(mn);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(t){this._hideRequiredMarker=fi(t)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||OT}set floatLabel(t){t!==this._floatLabel&&(this._floatLabel=t,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(t){let r=t||this._defaults?.appearance||O0;this._appearanceSignal.set(r)}_appearanceSignal=E(O0);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||F0}set subscriptSizing(t){this._subscriptSizing=t||this._defaults?.subscriptSizing||F0}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(t){this._hintLabel=t,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(t){this._explicitFormFieldControl=t}_destroyed=new q;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=_t();constructor(){let t=this._defaults,r=f(Wa);t&&(t.appearance&&(this.appearance=t.appearance),this._hideRequiredMarker=!!t?.hideRequiredMarker,t.color&&(this.color=t.color)),_n(()=>this._currentDirection=r.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=ts(this._control.ngField)?null:this._control.ngControl.control.validator),this._previousControl=this._control,this._changeDetectorRef.markForCheck()),this._control.ngControl&&this._control.ngControl.control&&!ts(this._control.ngField)&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=$e(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(t){let r=this._control,o="mat-mdc-form-field-type-";t&&this._elementRef.nativeElement.classList.remove(o+t.controlType),r.controlType&&this._elementRef.nativeElement.classList.add(o+r.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=r.stateChanges?.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=r.stateChanges?.pipe(Ti([void 0,void 0]),be(()=>[this._unwrapMaybeSignal(r.errorState),r.userAriaDescribedBy]),Hd(),dt(([[i,a],[s,c]])=>i!==s||a!==c)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),r.ngControl&&r.ngControl.valueChanges&&!ts(r.ngField)&&(this._valueChanges=r.ngControl.valueChanges.pipe(Jt(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(t=>!t._isText),this._hasTextPrefix=!!this._prefixChildren.find(t=>t._isText),this._hasIconSuffix=!!this._suffixChildren.find(t=>!t._isText),this._hasTextSuffix=!!this._suffixChildren.find(t=>t._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),jd(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let t=this._unwrapMaybeSignal(this._control.focused);t&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!t&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",t),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",t)}_syncOutlineLabelOffset(){Sm({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let t of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(t,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:t=>this._writeOutlinedLabelStyles(t())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=$e(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._shouldAlwaysFloat()||this._unwrapMaybeSignal(this._control.shouldLabelFloat):!1}_shouldForward(t){let r=this._control?.ngField||this._control?.ngControl;if(!r)return!1;if(ts(r)){let o=r();return t==="valid"?o.valid():t==="dirty"?o.dirty():t==="touched"?o.touched():t==="pending"?o.pending():t==="untouched"?!o.touched():t==="pristine"?!o.dirty():t==="invalid"?!o.valid():!1}else{let o=r;return t==="valid"?o.valid:t==="dirty"?o.dirty:t==="touched"?o.touched:t==="pending"?o.pending:t==="untouched"?o.untouched:t==="pristine"?o.pristine:t==="invalid"?o.invalid:!1}}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._unwrapMaybeSignal(this._control.errorState)?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let t=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&t.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let i=this._hintChildren?this._hintChildren.find(s=>s.align==="start"):null,a=this._hintChildren?this._hintChildren.find(s=>s.align==="end"):null;i?t.push(i.id):this._hintLabel&&t.push(this._hintLabelId),a&&t.push(a.id)}else this._errorChildren&&t.push(...this._errorChildren.map(i=>i.id));let r=this._control.describedByIds,o;if(r){let i=this._describedByIds||t;o=t.concat(r.filter(a=>a&&!i.includes(a)))}else o=t;this._control.setDescribedByIds(o),this._describedByIds=t}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let t=this._iconPrefixContainer?.nativeElement,r=this._textPrefixContainer?.nativeElement,o=this._iconSuffixContainer?.nativeElement,i=this._textSuffixContainer?.nativeElement,a=t?.getBoundingClientRect().width??0,s=r?.getBoundingClientRect().width??0,c=o?.getBoundingClientRect().width??0,l=i?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",u=`${a+s}px`,h=`calc(${d} * (${u} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,_=`var(--mat-mdc-form-field-label-transform, ${FT} translateX(${h}))`,D=a+s+c+l;return[_,D]}_writeOutlinedLabelStyles(t){if(t!==null){let[r,o]=t;this._floatingLabel&&(this._floatingLabel.element.style.transform=r),o!==null&&this._notchedOutline?._setMaxWidth(o)}}_isAttachedToDom(){let t=this._elementRef.nativeElement;if(t.getRootNode){let r=t.getRootNode();return r&&r!==t}return document.documentElement.contains(t)}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=(function(){let t=["iconPrefixContainer"],r=["textPrefixContainer"],o=["iconSuffixContainer"],i=["textSuffixContainer"],a=["textField"],s=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],c=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function l(W,ve){W&1&&oe(0,"span",21)}function d(W,ve){if(W&1&&(p(0,"label",20),Ce(1,1),M(2,l,1,0,"span",21),m()),W&2){let S=x(2);R("floating",S._shouldLabelFloat())("monitorResize",S._hasOutline())("id",S._labelId),ge("for",S._control.disableAutomaticLabeling?null:S._control.id),y(2),N(!S.hideRequiredMarker&&S._unwrapMaybeSignal(S._control.required)?2:-1)}}function u(W,ve){if(W&1&&M(0,d,3,5,"label",20),W&2){let S=x();N(S._hasFloatingLabel()?0:-1)}}function g(W,ve){W&1&&oe(0,"div",7)}function h(W,ve){}function _(W,ve){if(W&1&&Yo(0,h,0,0,"ng-template",13),W&2){x(2);let S=mr(1);R("ngTemplateOutlet",S)}}function D(W,ve){if(W&1&&(p(0,"div",9),M(1,_,1,1,null,13),m()),W&2){let S=x();R("matFormFieldNotchedOutlineOpen",S._shouldLabelFloat()),y(),N(S._forceDisplayInfixLabel()?-1:1)}}function A(W,ve){W&1&&(p(0,"div",10,2),Ce(2,2),m())}function j(W,ve){W&1&&(p(0,"div",11,3),Ce(2,3),m())}function J(W,ve){}function Je(W,ve){if(W&1&&Yo(0,J,0,0,"ng-template",13),W&2){x();let S=mr(1);R("ngTemplateOutlet",S)}}function $0(W,ve){W&1&&(p(0,"div",14,4),Ce(2,4),m())}function G0(W,ve){W&1&&(p(0,"div",15,5),Ce(2,5),m())}function W0(W,ve){W&1&&oe(0,"div",16)}function q0(W,ve){W&1&&(p(0,"div",18),Ce(1,6),m())}function K0(W,ve){if(W&1&&(p(0,"mat-hint",22),b(1),m()),W&2){let S=x(2);R("id",S._hintLabelId),y(),le(S.hintLabel)}}function Z0(W,ve){if(W&1&&(p(0,"div",19),M(1,K0,2,2,"mat-hint",22),Ce(2,7),oe(3,"div",23),Ce(4,8),m()),W&2){let S=x();y(),N(S.hintLabel?1:-1)}}return Q({type:e,selectors:[["mat-form-field"]],contentQueries:function(ve,S,_e){if(ve&1&&(Gc(_e,S._labelChild,mn,5),no(_e,th,5)(_e,NT,5)(_e,TT,5)(_e,MT,5)(_e,eh,5)),ve&2){qc();let ct;Te(ct=Ae())&&(S._formFieldControl=ct.first),Te(ct=Ae())&&(S._prefixChildren=ct),Te(ct=Ae())&&(S._suffixChildren=ct),Te(ct=Ae())&&(S._errorChildren=ct),Te(ct=Ae())&&(S._hintChildren=ct)}},viewQuery:function(ve,S){if(ve&1&&(Wc(S._iconPrefixContainerSignal,t,5)(S._textPrefixContainerSignal,r,5)(S._iconSuffixContainerSignal,o,5)(S._textSuffixContainerSignal,i,5),qn(a,5)(t,5)(r,5)(o,5)(i,5)(N0,5)(R0,5)(A0,5)),ve&2){qc(4);let _e;Te(_e=Ae())&&(S._textField=_e.first),Te(_e=Ae())&&(S._iconPrefixContainer=_e.first),Te(_e=Ae())&&(S._textPrefixContainer=_e.first),Te(_e=Ae())&&(S._iconSuffixContainer=_e.first),Te(_e=Ae())&&(S._textSuffixContainer=_e.first),Te(_e=Ae())&&(S._floatingLabel=_e.first),Te(_e=Ae())&&(S._notchedOutline=_e.first),Te(_e=Ae())&&(S._lineRipple=_e.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(ve,S){ve&2&&ce("mat-mdc-form-field-label-always-float",S._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",S._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",S._hasIconSuffix)("mat-form-field-invalid",S._unwrapMaybeSignal(S._control.errorState))("mat-form-field-disabled",S._unwrapMaybeSignal(S._control.disabled))("mat-form-field-autofilled",S._unwrapMaybeSignal(S._control.autofilled))("mat-form-field-appearance-fill",S.appearance=="fill")("mat-form-field-appearance-outline",S.appearance=="outline")("mat-form-field-hide-placeholder",S._hasFloatingLabel()&&!S._shouldLabelFloat())("mat-primary",S.color!=="accent"&&S.color!=="warn")("mat-accent",S.color==="accent")("mat-warn",S.color==="warn")("ng-untouched",S._shouldForward("untouched"))("ng-touched",S._shouldForward("touched"))("ng-pristine",S._shouldForward("pristine"))("ng-dirty",S._shouldForward("dirty"))("ng-valid",S._shouldForward("valid"))("ng-invalid",S._shouldForward("invalid"))("ng-pending",S._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[ft([{provide:nh,useExisting:e},{provide:P0,useExisting:e}])],ngContentSelectors:c,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(ve,S){if(ve&1&&(vt(s),Yo(0,u,1,1,"ng-template",null,0,hm),p(2,"div",6,1),w("click",function(ct){return S._control.onContainerClick(ct)}),M(4,g,1,0,"div",7),p(5,"div",8),M(6,D,2,2,"div",9),M(7,A,3,0,"div",10),M(8,j,3,0,"div",11),p(9,"div",12),M(10,Je,1,1,null,13),Ce(11),m(),M(12,$0,3,0,"div",14),M(13,G0,3,0,"div",15),m(),M(14,W0,1,0,"div",16),m(),p(15,"div",17),M(16,q0,2,0,"div",18)(17,Z0,5,1,"div",19),m()),ve&2){let _e,ct=S._unwrapMaybeSignal(S._control.disabled);y(2),ce("mdc-text-field--filled",!S._hasOutline())("mdc-text-field--outlined",S._hasOutline())("mdc-text-field--no-label",!S._hasFloatingLabel())("mdc-text-field--disabled",ct)("mdc-text-field--invalid",S._unwrapMaybeSignal(S._control.errorState)),y(2),N(!S._hasOutline()&&!ct?4:-1),y(2),N(S._hasOutline()?6:-1),y(),N(S._hasIconPrefix?7:-1),y(),N(S._hasTextPrefix?8:-1),y(2),N(!S._hasOutline()||S._forceDisplayInfixLabel()?10:-1),y(2),N(S._hasTextSuffix?12:-1),y(),N(S._hasIconSuffix?13:-1),y(),N(S._hasOutline()?-1:14),y(),ce("mat-mdc-form-field-subscript-dynamic-size",S.subscriptSizing==="dynamic");let Y0=S._getSubscriptMessageType();y(),N((_e=Y0)==="error"?16:_e==="hint"?17:-1)}},dependencies:[N0,R0,Im,A0,eh],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--%NS%mat-form-field-filled-input-text-color, var(--%NS%mat-sys-on-surface));
  caret-color: var(--%NS%mat-form-field-filled-caret-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--%NS%mat-form-field-outlined-input-text-color, var(--%NS%mat-sys-on-surface));
  caret-color: var(--%NS%mat-form-field-outlined-caret-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--%NS%mat-form-field-filled-error-caret-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--%NS%mat-form-field-outlined-error-caret-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--%NS%mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--%NS%mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-filled-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-top-right-radius: var(--%NS%mat-form-field-filled-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) {
  background-color: var(--%NS%mat-form-field-filled-container-color, var(--%NS%mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--%NS%mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-focus-label-text-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-hover-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-focus-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-hover-label-text-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--%NS%mat-form-field-filled-label-text-font, var(--%NS%mat-sys-body-large-font));
  font-size: var(--%NS%mat-form-field-filled-label-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-form-field-filled-label-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-form-field-filled-label-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-focus-label-text-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-hover-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-focus-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-hover-label-text-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--%NS%mat-form-field-outlined-label-text-font, var(--%NS%mat-sys-body-large-font));
  font-size: var(--%NS%mat-form-field-outlined-label-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-form-field-outlined-label-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-form-field-outlined-label-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--%NS%required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--%NS%required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-outline-color, var(--%NS%mat-sys-outline));
  border-width: var(--%NS%mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-hover-outline-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-focus-outline-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-outline-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-hover-outline-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-focus-outline-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--%NS%mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--%NS%mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--%NS%mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-active-indicator-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-hover-active-indicator-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-active-indicator-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-hover-active-indicator-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--%NS%mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--%NS%mat-form-field-filled-focus-active-indicator-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-focus-active-indicator-color, var(--%NS%mat-sys-error));
}

.mdc-line-ripple--%NS%active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--%NS%deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--%NS%no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --%NS%mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--%NS%mat-form-field-container-height, 56px);
  padding-top: var(--%NS%mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--%NS%mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--%NS%mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--%NS%mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--%NS%mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--%NS%mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --%NS%mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--%NS%mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--%NS%mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--%NS%mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--%NS%mat-form-field-error-text-color, var(--%NS%mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--%NS%mat-form-field-subscript-text-font, var(--%NS%mat-sys-body-small-font));
  line-height: var(--%NS%mat-form-field-subscript-text-line-height, var(--%NS%mat-sys-body-small-line-height));
  font-size: var(--%NS%mat-form-field-subscript-text-size, var(--%NS%mat-sys-body-small-size));
  letter-spacing: var(--%NS%mat-form-field-subscript-text-tracking, var(--%NS%mat-sys-body-small-tracking));
  font-weight: var(--%NS%mat-form-field-subscript-text-weight, var(--%NS%mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--%NS%mat-form-field-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--%NS%mat-form-field-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--%NS%mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--%NS%mat-form-field-select-option-text-color, var(--%NS%mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--%NS%mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--%NS%mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--%NS%mat-form-field-enabled-select-arrow-color, var(--%NS%mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--%NS%mat-form-field-focus-select-arrow-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--%NS%mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --%NS%mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--%NS%mat-form-field-container-text-font, var(--%NS%mat-sys-body-large-font));
  line-height: var(--%NS%mat-form-field-container-text-line-height, var(--%NS%mat-sys-body-large-line-height));
  font-size: var(--%NS%mat-form-field-container-text-size, var(--%NS%mat-sys-body-large-size));
  letter-spacing: var(--%NS%mat-form-field-container-text-tracking, var(--%NS%mat-sys-body-large-tracking));
  font-weight: var(--%NS%mat-form-field-container-text-weight, var(--%NS%mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--%NS%mat-form-field-outlined-label-text-populated-size) * var(--%NS%mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--%NS%mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--%NS%mat-form-field-leading-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--%NS%mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-trailing-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-trailing-icon-color, var(--%NS%mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-hover-trailing-icon-color, var(--%NS%mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-focus-trailing-icon-color, var(--%NS%mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--%NS%filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2})})()}return e})();var pn=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=ke({type:e});static \u0275inj=Ee({imports:[ex,Tn,Kt]})}return e})();var PT=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275cmp=Q({type:e,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(r,o){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2})}return e})(),LT={passive:!0},L0=(()=>{class e{_platform=f(at);_ngZone=f(Y);_renderer=f(It).createRenderer(null,null);_styleLoader=f(un);_monitoredElements=new Map;monitor(t){if(!this._platform.isBrowser)return Fe;this._styleLoader.load(PT);let r=dn(t),o=this._monitoredElements.get(r);if(o)return o.subject;let i=new q,a="cdk-text-field-autofilled",s=l=>{l.animationName==="cdk-text-field-autofill-start"&&!r.classList.contains(a)?(r.classList.add(a),this._ngZone.run(()=>i.next({target:l.target,isAutofilled:!0}))):l.animationName==="cdk-text-field-autofill-end"&&r.classList.contains(a)&&(r.classList.remove(a),this._ngZone.run(()=>i.next({target:l.target,isAutofilled:!1})))},c=this._ngZone.runOutsideAngular(()=>(r.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(r,"animationstart",s,LT)));return this._monitoredElements.set(r,{subject:i,unlisten:c}),i}stopMonitoring(t){let r=dn(t),o=this._monitoredElements.get(r);o&&(o.unlisten(),o.subject.complete(),r.classList.remove("cdk-text-field-autofill-monitored"),r.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(r))}ngOnDestroy(){this._monitoredElements.forEach((t,r)=>this.stopMonitoring(r))}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})();var V0=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=ke({type:e});static \u0275inj=Ee({})}return e})();var j0=new C("MAT_INPUT_VALUE_ACCESSOR");var B0=(()=>{class e{isErrorState(t,r){return!!(t&&t.invalid&&(t.touched||r&&r.submitted))}isSignalErrorState(t){if(!t)return!1;let r=t().invalid(),o=t().touched();return r&&o}static \u0275fac=function(r){return new(r||e)};static \u0275prov=B({token:e,factory:e.\u0275fac})}return e})();var ad=class{_defaultMatcher;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;ngControl;formField;constructor(n,t,r,o,i){this._defaultMatcher=n,this._parentFormGroup=r,this._parentForm=o,this._stateChanges=i,t?xn(t.field)&&!t.updateValueAndValidity?(this.formField=t,this.ngControl=null):(this.formField=null,this.ngControl=t):this.ngControl=this.formField=null}updateErrorState(){let n=this.errorState,t=this._getCurrentErrorState(this.matcher||this._defaultMatcher);t!==n&&(this.errorState=t,this._stateChanges.next())}_getCurrentErrorState(n){if(this.formField&&n?.isSignalErrorState)return n.isSignalErrorState(this.formField.field())??!1;let t=this._parentFormGroup||this._parentForm,r=this.ngControl?this.ngControl.control:null;return n?.isErrorState(r,t)??!1}};var VT=["button","checkbox","file","hidden","image","radio","range","reset","submit"],jT=new C("MAT_INPUT_CONFIG"),Cr=(()=>{class e{_elementRef=f(ue);_platform=f(at);ngControl=f(ql,{optional:!0,self:!0});_autofillMonitor=f(L0);_ngZone=f(Y);_formField=f(nh,{optional:!0});_renderer=f(Qe);_uid=f(At).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=f(jT,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new q;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(t){this._disabled=fi(t),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(t){this._id=t||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(Xa.required)??!1}set required(t){this._required=fi(t)}_required;get type(){return this._type}set type(t){this._type=t||"text",this._validateType(),!this._isTextarea&&vp().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(t){this._errorStateTracker.matcher=t}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(t){t!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(t):this._inputValueAccessor.value=t,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(t){this._readonly=fi(t)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(t){this._errorStateTracker.errorState=t}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(t=>vp().has(t));constructor(){let t=f(Ap,{optional:!0}),r=f(Rp,{optional:!0}),o=f(B0),i=f(j0,{optional:!0,self:!0}),a=f(Yl,{optional:!0,self:!0}),s=this._elementRef.nativeElement,c=s.nodeName.toLowerCase();i?xn(i.value)?this._signalBasedValueAccessor=i:this._inputValueAccessor=i:this._inputValueAccessor=s,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(s,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new ad(o,a||this.ngControl,r,t,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=c==="select",this._isTextarea=c==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=s.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&_n(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(t=>{this.autofilled=t.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(t){this._elementRef.nativeElement.focus(t)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(t){if(t!==this.focused){if(!this._isNativeSelect&&t&&this.disabled&&this.disabledInteractive){let r=this._elementRef.nativeElement;r.type==="number"?(r.type="text",r.setSelectionRange(0,0),r.type="number"):r.setSelectionRange(0,0)}this.focused=t,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let t=this._elementRef.nativeElement.value;this._previousNativeValue!==t&&(this._previousNativeValue=t,this.stateChanges.next())}_dirtyCheckPlaceholder(){let t=this._getPlaceholder();if(t!==this._previousPlaceholder){let r=this._elementRef.nativeElement;this._previousPlaceholder=t,t?r.setAttribute("placeholder",t):r.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){VT.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let t=this._elementRef.nativeElement.validity;return t&&t.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let t=this._elementRef.nativeElement,r=t.options[0];return this.focused||t.multiple||!this.empty||!!(t.selectedIndex>-1&&r&&r.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(t){let r=this._elementRef.nativeElement;t.length?r.setAttribute("aria-describedby",t.join(" ")):r.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let t=this._elementRef.nativeElement;return this._isNativeSelect&&(t.multiple||t.size>1)}_iOSKeyupListener=t=>{let r=t.target;!r.value&&r.selectionStart===0&&r.selectionEnd===0&&(r.setSelectionRange(1,1),r.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(r){return new(r||e)};static \u0275dir=re({type:e,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(r,o){r&1&&w("focus",function(){return o._focusChanged(!0)})("blur",function(){return o._focusChanged(!1)})("input",function(){return o._onInput()}),r&2&&(fr("id",o.id)("disabled",o.disabled&&!o.disabledInteractive)("required",o.required),ge("name",o.name||null)("readonly",o._getReadonlyAttribute())("aria-disabled",o.disabled&&o.disabledInteractive?"true":null)("aria-invalid",o.empty&&o.required?null:o.errorState)("aria-required",o.required)("id",o.id),ce("mat-input-server",o._isServer)("mat-mdc-form-field-textarea-control",o._isInFormField&&o._isTextarea)("mat-mdc-form-field-input-control",o._isInFormField)("mat-mdc-input-disabled-interactive",o.disabledInteractive)("mdc-text-field__input",o._isInFormField)("mat-mdc-native-select-inline",o._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",ie]},exportAs:["matInput"],features:[ft([{provide:th,useExisting:e}]),yt]})}return e})(),Sr=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=ke({type:e});static \u0275inj=Ee({imports:[pn,pn,V0,Kt]})}return e})();var BT=new Intl.NumberFormat("de-DE",{style:"currency",currency:"EUR"});function cd(e){return BT.format(e/100)}function U0(e){let n=e.trim().replace(/\s|€/g,"");if(!/^[-+]?\d{1,3}(\.\d{3})*(,\d{1,2})?$|^[-+]?\d+(,\d{1,2})?$/.test(n))return null;let t=n.startsWith("-"),[r,o=""]=n.replace(/^[-+]/,"").replace(/\./g,"").split(","),i=Number(r)*100+Number(o.padEnd(2,"0"));return Number.isSafeInteger(i)?t?-i:i:null}var Yt=class e{entries=E([]);total=$e(()=>this.entries().reduce((n,t)=>n+t.amount,0));vault=f(Re);async load(){this.entries.set(await this.vault.read("testdata","eintraege",[]))}async add(n){let t=[T(v({},n),{id:crypto.randomUUID()}),...this.entries()].sort((r,o)=>o.date.localeCompare(r.date));await this.vault.write("testdata","eintraege",t),this.entries.set(t)}async remove(n){let t=this.entries().filter(r=>r.id!==n);await this.vault.write("testdata","eintraege",t),this.entries.set(t)}clear(){this.entries.set([])}static \u0275fac=function(t){return new(t||e)};static \u0275prov=G({token:e,factory:e.\u0275fac,providedIn:"root"})};function UT(e,n){if(e&1&&(p(0,"p",10),b(1),m()),e&2){let t=x();y(),le(t.error())}}function HT(e,n){if(e&1&&(p(0,"p",11),b(1),m()),e&2){let t=x();y(),le(t.saved())}}var zT=()=>{let e=new Date;return`${e.getFullYear()}-${String(e.getMonth()+1).padStart(2,"0")}-${String(e.getDate()).padStart(2,"0")}`},ld=class e{store=f(Yt);router=f(He);kind=E("aus");error=E("");saved=E("");amount="";text="";date=zT();val(n){return n.target.value}async save(n){this.error.set("");let t=U0(this.amount);if(t===null||t===0)return this.error.set("Bitte einen Betrag wie 12,50 eingeben.");if(!this.text.trim())return this.error.set("Bitte einen kurzen Text eingeben.");if(!/^\d{4}-\d{2}-\d{2}$/.test(this.date))return this.error.set("Bitte ein Datum w\xE4hlen.");let r=this.kind()==="aus"?-Math.abs(t):Math.abs(t);await this.store.add({amount:r,text:this.text.trim(),date:this.date}),n?(this.saved.set(`Gespeichert: ${this.text.trim()} ${cd(r)}`),this.amount="",this.text=""):this.router.navigateByUrl("/start")}static \u0275fac=function(t){return new(t||e)};static \u0275cmp=Q({type:e,selectors:[["app-new-entry-page"]],decls:32,vars:6,consts:[[1,"page"],[1,"muted","small"],[1,"card"],["aria-label","Art",1,"kind",3,"change","value"],["value","aus"],["value","ein"],["appearance","outline"],["matInput","","data-testid","amount","inputmode","decimal","autocomplete","off","placeholder","z. B. 12,50",3,"input","value"],["matInput","","data-testid","text","autocomplete","off","placeholder","z. B. B\xE4ckerei",3,"input","value"],["matInput","","type","date","data-testid","date",3,"input","value"],["role","alert",1,"bad"],["role","status",1,"hint"],[1,"actions"],["mat-flat-button","","type","button","data-testid","save",3,"click"],["mat-stroked-button","","type","button",3,"click"],["mat-button","","routerLink","/start"]],template:function(t,r){t&1&&(p(0,"div",0)(1,"h1"),b(2,"Neue Buchung"),m(),p(3,"p",1),b(4," Testbuchung f\xFCr Version 0.1. Wenn du hier \xFCber den Schnellzugriff gelandet bist: Der Schnellzugriff funktioniert. "),m(),p(5,"div",2)(6,"mat-button-toggle-group",3),w("change",function(i){return r.kind.set(i.value)}),p(7,"mat-button-toggle",4),b(8,"Ausgabe"),m(),p(9,"mat-button-toggle",5),b(10,"Einnahme"),m()(),p(11,"mat-form-field",6)(12,"mat-label"),b(13,"Betrag in \u20AC"),m(),p(14,"input",7),w("input",function(i){return r.amount=r.val(i)}),m()(),p(15,"mat-form-field",6)(16,"mat-label"),b(17,"Text"),m(),p(18,"input",8),w("input",function(i){return r.text=r.val(i)}),m()(),p(19,"mat-form-field",6)(20,"mat-label"),b(21,"Datum"),m(),p(22,"input",9),w("input",function(i){return r.date=r.val(i)}),m()(),M(23,UT,2,1,"p",10),M(24,HT,2,1,"p",11),p(25,"div",12)(26,"button",13),w("click",function(){return r.save(!1)}),b(27," Speichern "),m(),p(28,"button",14),w("click",function(){return r.save(!0)}),b(29," Speichern und weitere "),m(),p(30,"a",15),b(31,"Abbrechen"),m()()()()),t&2&&(y(6),R("value",r.kind()),y(8),R("value",r.amount),y(4),R("value",r.text),y(4),R("value",r.date),y(),N(r.error()?23:-1),y(),N(r.saved()?24:-1))},dependencies:[Ct,xt,k0,Xp,rd,pn,Tn,mn,Sr,Cr,it],styles:[".kind[_ngcontent-%COMP%]{margin:0 0 16px}"]})};var $T={lock:"M7 11V8a5 5 0 0 1 10 0v3M5 11h14v10H5z",settings:"M12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6zM19.4 15a1.7 1.7 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.7 1.7 0 0 0-2.8 1.2V21a2 2 0 1 1-4 0v-.1a1.7 1.7 0 0 0-2.9-1.2l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1A1.7 1.7 0 0 0 3 15.1H3a2 2 0 1 1 0-4h.1a1.7 1.7 0 0 0 1.2-2.9l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1A1.7 1.7 0 0 0 9 4.6V3a2 2 0 1 1 4 0v.1a1.7 1.7 0 0 0 2.9 1.2l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1A1.7 1.7 0 0 0 20.9 9H21a2 2 0 1 1 0 4h-.1a1.7 1.7 0 0 0-1.5 2z",fingerprint:"M12 11v3a8 8 0 0 1-1.5 4.7M8.5 7.5A5 5 0 0 1 17 11v2M7 11a5 5 0 0 0 0 0v3a10 10 0 0 0 .8 4M4.5 9a8 8 0 0 1 15 2v3M15.5 17.5a14 14 0 0 1-1 3.5",plus:"M12 5v14M5 12h14",back:"M15 18l-6-6 6-6",check:"M5 12l5 5L20 7",trash:"M4 7h16M9 7V4h6v3M6 7l1 13h10l1-13",download:"M12 3v12M7 10l5 5 5-5M5 21h14",share:"M4 12v7a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-7M16 6l-4-4-4 4M12 2v13"},Qt=class e{name=En.required();size=En(22);d=$e(()=>$T[this.name()]??"");static \u0275fac=function(t){return new(t||e)};static \u0275cmp=Q({type:e,selectors:[["app-icon"]],inputs:{name:[1,"name"],size:[1,"size"]},decls:2,vars:3,consts:[["viewBox","0 0 24 24","fill","none","stroke","currentColor","stroke-width","2","stroke-linecap","round","stroke-linejoin","round","aria-hidden","true"]],template:function(t,r){t&1&&(Lo(),zt(0,"svg",0),Mt(1,"path"),$t()),t&2&&(ge("width",r.size())("height",r.size()),y(),ge("d",r.d()))},styles:["[_nghost-%COMP%]{display:inline-flex;vertical-align:middle}"]})};function GT(e,n){if(e&1){let t=fe();p(0,"button",12),w("click",function(){P(t);let o=x();return L(o.backup("share"))}),oe(1,"app-icon",16),b(2," Backup teilen "),m()}if(e&2){let t=x();R("disabled",t.busy())}}function WT(e,n){if(e&1&&(p(0,"p",8),b(1),m()),e&2){let t=x();y(),le(t.backupMsg())}}function qT(e,n){if(e&1){let t=fe();p(0,"mat-form-field",9)(1,"mat-label"),b(2,"Bisheriges Passwort"),m(),p(3,"input",17),w("input",function(o){P(t);let i=x();return L(i.oldPw=i.val(o))}),m()()}if(e&2){let t=x();y(3),R("value",t.oldPw)}}function KT(e,n){if(e&1&&(p(0,"p",18),b(1),m()),e&2){let t=x();ro(t.pwOk()?"hint":"bad"),y(),le(t.pwMsg())}}function ZT(e,n){if(e&1){let t=fe();p(0,"button",19),w("click",function(){P(t);let o=x();return L(o.confirmDelete.set(!0))}),b(1," L\xF6schen \u2026 "),m()}}function YT(e,n){if(e&1){let t=fe();p(0,"mat-form-field",9)(1,"mat-label"),b(2,"Zur Best\xE4tigung L\xD6SCHEN eintippen"),m(),p(3,"input",20),w("input",function(o){P(t);let i=x();return L(i.del=i.val(o))}),m()(),p(4,"div",3)(5,"button",21),w("click",function(){P(t);let o=x();return L(o.deleteAll())}),b(6," Endg\xFCltig l\xF6schen "),m(),p(7,"button",22),w("click",function(){P(t);let o=x();return L(o.confirmDelete.set(!1))}),b(8,"Abbrechen"),m()()}if(e&2){let t=x();y(3),R("value",t.del),y(2),R("disabled",t.del.trim().toUpperCase()!=="L\xD6SCHEN")}}var dd=class e{vault=f(Re);router=f(He);version=Si;minPw=fn;busy=E(!1);backupMsg=E("");pwMsg=E("");pwOk=E(!1);confirmDelete=E(!1);canShare=typeof navigator.canShare=="function";pw1="";pw2="";oldPw="";del="";val(n){return n.target.value}async backup(n){this.busy.set(!0),this.backupMsg.set("");try{let{name:t,text:r,entries:o}=await this.vault.createBackupFile(),i=`Backup \u201E${t}\u201C erstellt (${o} ${o===1?"Datensatz":"Datens\xE4tze"}).`;if(n==="share"){let c=new File([r],t,{type:"application/octet-stream"});if(navigator.canShare?.({files:[c]}))try{await navigator.share({files:[c],title:t}),this.backupMsg.set(i);return}catch(l){if(l.name==="AbortError")return}this.backupMsg.set("Teilen klappt auf diesem Ger\xE4t nicht, die Datei wird stattdessen heruntergeladen.")}let a=URL.createObjectURL(new Blob([r],{type:"application/octet-stream"})),s=document.createElement("a");s.href=a,s.download=t,document.body.appendChild(s),s.click(),s.remove(),setTimeout(()=>URL.revokeObjectURL(a),1e4),this.backupMsg.set(`${i} Du findest es im Ordner \u201EDownloads\u201C.`)}catch(t){this.backupMsg.set(`Backup fehlgeschlagen: ${t instanceof Error?t.message:String(t)}`)}finally{this.busy.set(!1)}}async setPassword(){if(this.pwOk.set(!1),this.pw1.length<fn)return this.pwMsg.set(`Mindestens ${fn} Zeichen, z. B. ein Satz aus mehreren W\xF6rtern.`);if(this.pw1!==this.pw2)return this.pwMsg.set("Die beiden Passw\xF6rter sind nicht gleich.");this.busy.set(!0);try{await this.vault.setPassword(this.pw1,this.vault.meta()?.passkey?{passkey:!0}:{password:this.oldPw}),this.pw1=this.pw2=this.oldPw="",this.pwOk.set(!0),this.pwMsg.set("App-Passwort gespeichert.")}catch(n){this.pwMsg.set(n instanceof Error?n.message:String(n))}finally{this.busy.set(!1)}}async deleteAll(){await this.vault.deleteEverything(),this.router.navigateByUrl("/einrichtung")}static \u0275fac=function(t){return new(t||e)};static \u0275cmp=Q({type:e,selectors:[["app-settings-page"]],decls:51,vars:18,consts:[[1,"page"],[1,"card"],[1,"small"],[1,"actions"],["mat-flat-button","","type","button","data-testid","backup-download",3,"click","disabled"],["name","download"],["mat-stroked-button","","type","button",3,"disabled"],[1,"muted","small"],["role","status",1,"hint"],["appearance","outline"],["matInput","","type","password","autocomplete","new-password",3,"input","value"],["role","status",3,"class"],["mat-stroked-button","","type","button",3,"click","disabled"],["mat-stroked-button","","routerLink","/pruefung"],[1,"small","muted"],["mat-button","","type","button",1,"danger"],["name","share"],["matInput","","type","password","autocomplete","current-password",3,"input","value"],["role","status"],["mat-button","","type","button",1,"danger",3,"click"],["matInput","","autocomplete","off",3,"input","value"],["mat-flat-button","","type","button",1,"danger-fill",3,"click","disabled"],["mat-button","","type","button",3,"click"]],template:function(t,r){t&1&&(p(0,"div",0)(1,"h1"),b(2,"Einstellungen"),m(),p(3,"div",1)(4,"h2"),b(5,"Backup"),m(),p(6,"p",2),b(7),m(),p(8,"div",3)(9,"button",4),w("click",function(){return r.backup("download")}),oe(10,"app-icon",5),b(11," Backup herunterladen "),m(),M(12,GT,3,1,"button",6),m(),p(13,"p",7),b(14," \u201ETeilen\u201C schickt die Datei zum Beispiel an Google Drive oder per E-Mail an dich selbst. F\xFCr den Test: Datei auf dem zweiten Handy \xF6ffnen und dort \u201EBackup einspielen\u201C w\xE4hlen. "),m(),M(15,WT,2,1,"p",8),m(),p(16,"div",1)(17,"h2"),b(18,"App-Passwort"),m(),p(19,"p",2),b(20),m(),M(21,qT,4,1,"mat-form-field",9),p(22,"mat-form-field",9)(23,"mat-label"),b(24),m(),p(25,"input",10),w("input",function(i){return r.pw1=r.val(i)}),m()(),p(26,"mat-form-field",9)(27,"mat-label"),b(28,"Wiederholen"),m(),p(29,"input",10),w("input",function(i){return r.pw2=r.val(i)}),m()(),M(30,KT,2,3,"p",11),p(31,"p",7),b(32),m(),p(33,"button",12),w("click",function(){return r.setPassword()}),b(34," Passwort speichern "),m()(),p(35,"div",1)(36,"h2"),b(37,"Ger\xE4t"),m(),p(38,"p",2),b(39),m(),p(40,"p",2),b(41,"Die App sperrt sich, wenn du sie l\xE4nger als eine Minute verl\xE4sst."),m(),p(42,"a",13),b(43,"Machbarkeitspr\xFCfung"),m()(),p(44,"div",1)(45,"h2"),b(46,"Alle Daten auf diesem Handy l\xF6schen"),m(),p(47,"p",14),b(48," Nur f\xFCr Tests oder vor dem Einspielen eines Backups. Ohne Backup sind die Daten danach weg. "),m(),M(49,ZT,2,0,"button",15)(50,YT,9,2),m()()),t&2&&(y(7),ot(" Das Backup ist eine verschl\xFCsselte Datei (.hhb). \xD6ffnen l\xE4sst sie sich nur mit deinem Wiederherstellungsschl\xFCssel",r.vault.meta()?.password?" oder deinem App-Passwort":"",". "),y(2),R("disabled",r.busy()),y(3),N(r.canShare?12:-1),y(3),N(r.backupMsg()?15:-1),y(5),ot(" ",r.vault.meta()?.password?"Ein App-Passwort ist eingerichtet. Du kannst es hier \xE4ndern.":"Noch kein App-Passwort. Es ist ein Ersatzweg, falls der Fingerabdruck einmal nicht klappt."," "),y(),N(!r.vault.meta()?.passkey&&r.vault.meta()?.password?21:-1),y(3),ot("Neues App-Passwort (mindestens ",r.minPw," Zeichen)"),y(),R("value",r.pw1),y(4),R("value",r.pw2),y(),N(r.pwMsg()?30:-1),y(2),oo(" Mindestens ",r.minPw," Zeichen.",r.vault.meta()?.passkey?" Zum Speichern fragt die App nach deinem Fingerabdruck.":""," "),y(),R("disabled",r.busy()),y(6),Kc(" Version ",r.version,". Entsperren: ",r.vault.meta()?.passkey?"Fingerabdruck":"","",r.vault.meta()?.passkey&&r.vault.meta()?.password?", ":"","",r.vault.meta()?.password?"App-Passwort":"",", Wiederherstellungsschl\xFCssel. "),y(10),N(r.confirmDelete()?50:49))},dependencies:[Ct,xt,pn,Tn,mn,Sr,Cr,it,Qt],styles:[".danger[_ngcontent-%COMP%]{color:var(--%NS%hb-bad-ink)}.danger-fill[_ngcontent-%COMP%]{background:var(--%NS%hb-bad-ink)!important;color:var(--%NS%hb-surface)!important}"]})};function QT(e,n){if(e&1){let t=fe();p(0,"h1"),b(1,"Willkommen im Haushaltsbuch"),m(),p(2,"p"),b(3," Alle Daten bleiben verschl\xFCsselt auf diesem Handy. Niemand sonst kann sie lesen, auch GitHub nicht. "),m(),p(4,"div",1)(5,"h2"),b(6,"Neu einrichten"),m(),p(7,"p",2),b(8," Vier kurze Schritte: Fingerabdruck, Wiederherstellungsschl\xFCssel aufschreiben, kurz kontrollieren, freiwillig ein App-Passwort. "),m(),p(9,"button",3),w("click",function(){P(t);let o=x();return L(o.begin())}),b(10," Neu einrichten "),m()(),p(11,"div",1)(12,"h2"),b(13,"Aus einem Backup wiederherstellen"),m(),p(14,"p",2),b(15," Zum Beispiel auf einem zweiten oder neuen Handy. Du brauchst die Backup-Datei (.hhb) und deinen Wiederherstellungsschl\xFCssel. "),m(),p(16,"button",4),w("click",function(){P(t);let o=x();return L(o.beginRestore())}),b(17," Backup einspielen "),m()(),p(18,"p",5)(19,"a",6),b(20,"Vorab pr\xFCfen, was dieses Handy kann"),m()()}}function XT(e,n){if(e&1&&(p(0,"p",9),b(1),m()),e&2){let t=x(2);y(),le(t.backupInfo())}}function JT(e,n){if(e&1&&(p(0,"p",12),b(1),m()),e&2){let t=x(2);y(),le(t.error())}}function eA(e,n){if(e&1){let t=fe();p(0,"h1"),b(1,"Backup einspielen"),m(),p(2,"div",1)(3,"label",7)(4,"strong"),b(5,"1. Backup-Datei ausw\xE4hlen"),m()(),p(6,"p")(7,"input",8),w("change",function(o){P(t);let i=x();return L(i.pickBackup(o))}),m()(),M(8,XT,2,1,"p",9),p(9,"mat-form-field",10)(10,"mat-label"),b(11,"2. Wiederherstellungsschl\xFCssel"),m(),p(12,"input",11),w("input",function(o){P(t);let i=x();return L(i.restoreKey=i.val(o))}),m()(),M(13,JT,2,1,"p",12),p(14,"div",13)(15,"button",14),w("click",function(){P(t);let o=x();return L(o.openBackup())}),b(16," Backup \xF6ffnen "),m(),p(17,"button",15),w("click",function(){P(t);let o=x();return L(o.go("start"))}),b(18,"Zur\xFCck"),m()()()}if(e&2){let t=x();y(8),N(t.backupInfo()?8:-1),y(4),R("value",t.restoreKey),y(),N(t.error()?13:-1),y(2),R("disabled",t.busy())}}function tA(e,n){if(e&1&&(p(0,"p",9),b(1),m()),e&2){let t=x(2);y(),le(t.backupInfo())}}function nA(e,n){if(e&1){let t=fe();p(0,"p",16),b(1),m(),p(2,"button",17),w("click",function(){P(t);let o=x(2);return L(o.withoutPasskey())}),b(3," Weiter mit App-Passwort "),m()}if(e&2){let t=x(2);y(),le(t.passkeyInfo())}}function rA(e,n){if(e&1){let t=fe();p(0,"p",12),b(1),m(),p(2,"button",15),w("click",function(){P(t);let o=x(3);return L(o.withoutPasskey())}),b(3," Ohne Fingerabdruck weiter "),m()}if(e&2){let t=x(3);y(),le(t.error())}}function oA(e,n){if(e&1){let t=fe();p(0,"button",18),w("click",function(){P(t);let o=x(2);return L(o.createPasskey())}),oe(1,"app-icon",19),b(2," Fingerabdruck einrichten "),m(),M(3,rA,4,1)}if(e&2){let t=x(2);R("disabled",t.busy()),y(3),N(t.error()?3:-1)}}function iA(e,n){if(e&1&&(p(0,"p",2),b(1),m(),p(2,"h1"),b(3,"Fingerabdruck einrichten"),m(),M(4,tA,2,1,"p",9),p(5,"div",1)(6,"p"),b(7," Gleich fragt Android nach deinem Fingerabdruck und ob ein "),p(8,"strong"),b(9,"Passkey"),m(),b(10," f\xFCr \u201EMein Haushaltsbuch\u201C gespeichert werden soll. Bitte best\xE4tigen. Manchmal fragt Android zweimal hintereinander, das ist normal. "),m(),M(11,nA,4,1)(12,oA,4,2),m()),e&2){let t=x();y(),oo("Schritt ",t.stepNo()," von ",t.total()),y(3),N(t.restoring()&&t.backupInfo()?4:-1),y(7),N(t.passkeyOk()===!1?11:12)}}function aA(e,n){if(e&1&&(p(0,"p",9),b(1),m()),e&2){let t=x(2);y(),le(t.passkeyInfo())}}function sA(e,n){if(e&1){let t=fe();p(0,"p",2),b(1,"Schritt 2 von 4"),m(),p(2,"h1"),b(3,"Wiederherstellungsschl\xFCssel"),m(),M(4,aA,2,1,"p",9),p(5,"div",1)(6,"p"),b(7," Das ist dein Generalschl\xFCssel. "),p(8,"strong"),b(9,"Schreib ihn jetzt auf Papier ab"),m(),b(10," und bewahre ihn sicher auf, zum Beispiel bei den Versicherungsunterlagen. "),m(),p(11,"p",20),b(12),oe(13,"br"),b(14),m(),p(15,"p",2),b(16," Du brauchst ihn, wenn das Handy kaputtgeht, der Fingerabdruck nicht mehr klappt oder du ein Backup auf einem anderen Handy \xF6ffnen willst. Niemand kann ihn dir wiederbeschaffen, auch ich nicht. Bitte keinen Screenshot: Der landet oft automatisch in der Cloud. "),m(),p(17,"button",21),w("click",function(){P(t);let o=x();return L(o.keyWritten())}),b(18," Ich habe ihn aufgeschrieben "),m()()}if(e&2){let t=x();y(4),N(t.passkeyInfo()?4:-1),y(8),ot(" ",t.groups().slice(0,3).join("-"),"-"),y(2),ot("",t.groups().slice(3).join("-")," ")}}function cA(e,n){if(e&1&&(p(0,"p",12),b(1),m()),e&2){let t=x(2);y(),le(t.error())}}function lA(e,n){if(e&1){let t=fe();p(0,"p",2),b(1,"Schritt 3 von 4"),m(),p(2,"h1"),b(3,"Kurze Kontrolle"),m(),p(4,"div",1)(5,"p"),b(6," Bitte tippe zwei Gruppen von deinem Zettel ab. So sehen wir, ob alles richtig abgeschrieben ist. "),m(),p(7,"mat-form-field",10)(8,"mat-label"),b(9),m(),p(10,"input",22),w("input",function(o){P(t);let i=x();return L(i.answer1=i.val(o))}),m()(),p(11,"mat-form-field",10)(12,"mat-label"),b(13),m(),p(14,"input",23),w("input",function(o){P(t);let i=x();return L(i.answer2=i.val(o))}),m()(),M(15,cA,2,1,"p",12),p(16,"div",13)(17,"button",24),w("click",function(){P(t);let o=x();return L(o.checkKey())}),b(18," Pr\xFCfen "),m(),p(19,"button",15),w("click",function(){P(t);let o=x();return L(o.go("key"))}),b(20,"Schl\xFCssel nochmal ansehen"),m()()()}if(e&2){let t=x();y(9),ot("Gruppe ",t.ask()[0]+1," von 6"),y(),R("value",t.answer1),ge("data-group",t.ask()[0]),y(3),ot("Gruppe ",t.ask()[1]+1," von 6"),y(),R("value",t.answer2),ge("data-group",t.ask()[1]),y(),N(t.error()?15:-1)}}function dA(e,n){e&1&&(p(0,"p"),b(1," Ein App-Passwort ist ein Ersatzweg, falls der Fingerabdruck einmal nicht klappt. Du kannst es auch sp\xE4ter in den Einstellungen festlegen. "),m())}function uA(e,n){e&1&&(p(0,"p"),b(1," Weil der Fingerabdruck hier nicht als Schl\xFCssel funktioniert, entsperrst du die App mit diesem Passwort. "),m())}function fA(e,n){if(e&1&&(p(0,"p",12),b(1),m()),e&2){let t=x(2);y(),le(t.error())}}function mA(e,n){if(e&1){let t=fe();p(0,"button",29),w("click",function(){P(t);let o=x(2);return L(o.finish(!1))}),b(1," Ohne Passwort fertig "),m()}if(e&2){let t=x(2);R("disabled",t.busy())}}function pA(e,n){if(e&1){let t=fe();p(0,"p",2),b(1),m(),p(2,"h1"),b(3),m(),p(4,"div",1),M(5,dA,2,0,"p")(6,uA,2,0,"p"),p(7,"mat-form-field",10)(8,"mat-label"),b(9,"App-Passwort"),m(),p(10,"input",25),w("input",function(o){P(t);let i=x();return L(i.pw1=i.val(o))}),m()(),p(11,"mat-form-field",10)(12,"mat-label"),b(13,"Passwort wiederholen"),m(),p(14,"input",26),w("input",function(o){P(t);let i=x();return L(i.pw2=i.val(o))}),m()(),p(15,"p",2),b(16," Tipp: ein Satz aus mehreren W\xF6rtern, z. B. \u201EHunde lieben Brennholz im Winter\u201C. Das Speichern dauert ein bis zwei Sekunden, weil das Passwort absichtlich aufwendig gesch\xFCtzt wird. "),m(),M(17,fA,2,1,"p",12),p(18,"div",13)(19,"button",27),w("click",function(){P(t);let o=x();return L(o.finish(!0))}),b(20),m(),M(21,mA,2,1,"button",28),m()()}if(e&2){let t=x();y(),oo("Schritt ",t.stepNo()," von ",t.total()),y(2),ot("App-Passwort",t.passkeyOk()?" (freiwillig)":""),y(2),N(t.passkeyOk()?5:6),y(5),R("value",t.pw1),y(4),R("value",t.pw2),y(3),N(t.error()?17:-1),y(2),R("disabled",t.busy()),y(),ot(" ",t.busy()?"Einen Moment \u2026":"Passwort festlegen und fertig"," "),y(),N(t.passkeyOk()?21:-1)}}function hA(e,n){if(e&1){let t=fe();p(0,"h1"),b(1,"Fertig eingerichtet"),m(),p(2,"div",1)(3,"p",9),b(4," Deine Daten sind verschl\xFCsselt gespeichert. Die App sperrt sich, wenn du sie l\xE4nger als eine Minute verl\xE4sst. "),m(),p(5,"p")(6,"strong"),b(7,"Noch ein Tipp:"),m(),b(8," Halte das App-Symbol auf dem Startbildschirm gedr\xFCckt und zieh \u201ENeue Buchung\u201C als eigenes Symbol auf den Startbildschirm. "),m(),p(9,"button",30),w("click",function(){P(t);let o=x();return L(o.toApp())}),b(10," Zur App "),m()()}}var ud=class e{vault=f(Re);store=f(Yt);router=f(He);step=E("start");busy=E(!1);error=E("");passkeyOk=E(null);passkeyInfo=E("");restoring=E(!1);backupInfo=E("");groups=E([]);ask=E([0,0]);minPw=fn;session=null;backup=null;answer1="";answer2="";pw1="";pw2="";restoreKey="";stepNo(){return this.restoring()?this.step()==="passkey"?1:2:{start:0,restore:0,passkey:1,key:2,confirm:3,password:4,done:4}[this.step()]}total(){return this.restoring()?2:4}async begin(){this.restoring.set(!1),this.session=await this.vault.startSetup(),this.groups.set(Xx(this.session.recoveryKey??"")),this.go("passkey")}beginRestore(){this.restoring.set(!0),this.go("restore")}async pickBackup(n){let t=n.target.files?.[0];if(this.error.set(""),this.backup=null,this.backupInfo.set(""),!!t)try{this.backup=s0(await t.text()),this.backupInfo.set(`Backup vom ${new Date(this.backup.createdAt).toLocaleString("de-DE")}, App-Version ${this.backup.appVersion}.`)}catch(r){this.error.set(r instanceof Zt?r.message:"Die Datei l\xE4sst sich nicht lesen.")}}async openBackup(){this.error.set("");let n=Ql(this.restoreKey);if(!this.backup)return this.error.set("Bitte zuerst die Backup-Datei ausw\xE4hlen.");if(!n)return this.error.set("Der Wiederherstellungsschl\xFCssel hat 30 Zeichen in sechs Gruppen, z. B. 7K2QX-\u2026");this.busy.set(!0);try{this.session=await this.vault.startRestore(this.backup,n);let t=Object.values(this.session.content?.stores??{}).reduce((r,o)=>r+o.length,0);this.backupInfo.set(`Backup ge\xF6ffnet (${t} ${t===1?"Datensatz":"Datens\xE4tze"}). Jetzt noch den Fingerabdruck f\xFCr dieses Handy einrichten.`),this.go("passkey")}catch(t){this.error.set(t instanceof Error?t.message:String(t))}finally{this.busy.set(!1)}}async createPasskey(){if(this.session){this.busy.set(!0),this.error.set("");try{await this.vault.addPasskey(this.session),this.passkeyOk.set(!0),this.passkeyInfo.set("Fingerabdruck eingerichtet. Damit entsperrst du die App ab jetzt."),this.go(this.restoring()?"password":"key")}catch(n){let t=n instanceof St?n:null;t?.reason==="kein-prf"||t?.reason==="nicht-verfuegbar"?(this.passkeyOk.set(!1),this.passkeyInfo.set(`${t.message} Kein Problem: Du entsperrst stattdessen mit einem App-Passwort. Bitte gib mir dieses Ergebnis weiter.`)):this.error.set(t?.message??String(n))}finally{this.busy.set(!1)}}}withoutPasskey(){this.passkeyOk.set(!1),this.passkeyInfo()||this.passkeyInfo.set("Ohne Fingerabdruck: Du entsperrst mit einem App-Passwort."),this.go(this.restoring()?"password":"key")}keyWritten(){let n=Math.floor(Math.random()*6),t=Math.floor(Math.random()*5);t>=n&&t++,this.ask.set(n<t?[n,t]:[t,n]),this.answer1="",this.answer2="",this.go("confirm")}checkKey(){let[n,t]=this.ask(),r=this.groups();Lp(this.answer1)===r[n]&&Lp(this.answer2)===r[t]?this.go("password"):this.error.set("Das stimmt noch nicht. Bitte mit deinem Zettel vergleichen oder den Schl\xFCssel nochmal ansehen.")}async finish(n){if(this.session){if(this.error.set(""),n){if(this.pw1.length<fn)return this.error.set(`Das App-Passwort braucht mindestens ${fn} Zeichen. Ein Satz aus mehreren W\xF6rtern ist gut zu merken.`);if(this.pw1!==this.pw2)return this.error.set("Die beiden Passw\xF6rter sind nicht gleich.")}this.busy.set(!0);try{await this.vault.finishSetup(this.session,n?this.pw1:null),this.session=null,this.pw1=this.pw2="",await this.store.load(),this.go("done")}catch(t){this.error.set(t instanceof Error?t.message:String(t))}finally{this.busy.set(!1)}}}toApp(){this.router.navigateByUrl("/start")}go(n){this.error.set(""),this.step.set(n),window.scrollTo(0,0)}val(n){return n.target.value}static \u0275fac=function(t){return new(t||e)};static \u0275cmp=Q({type:e,selectors:[["app-setup-page"]],decls:8,vars:1,consts:[[1,"page"],[1,"card"],[1,"muted","small"],["mat-flat-button","","type","button","data-testid","neu",1,"full",3,"click"],["mat-stroked-button","","type","button","data-testid","restore",1,"full",3,"click"],[1,"small"],["routerLink","/pruefung"],["for","backupFile",1,"small"],["id","backupFile","type","file","accept",".hhb,application/json,text/plain",3,"change"],[1,"hint"],["appearance","outline"],["matInput","","data-testid","restore-key","autocomplete","off","autocapitalize","characters","spellcheck","false","placeholder","XXXXX-XXXXX-XXXXX-XXXXX-XXXXX-XXXXX",3,"input","value"],["role","alert",1,"bad"],[1,"actions"],["mat-flat-button","","type","button","data-testid","open-backup",3,"click","disabled"],["mat-button","","type","button",3,"click"],["role","status",1,"warn"],["mat-flat-button","","type","button",1,"full",3,"click"],["mat-flat-button","","type","button","data-testid","passkey",1,"full",3,"click","disabled"],["name","fingerprint"],["data-testid","recovery-key",1,"key"],["mat-flat-button","","type","button","data-testid","key-written",1,"full",3,"click"],["matInput","","data-testid","group-a","autocomplete","off","autocapitalize","characters","spellcheck","false","maxlength","7",3,"input","value"],["matInput","","data-testid","group-b","autocomplete","off","autocapitalize","characters","spellcheck","false","maxlength","7",3,"input","value"],["mat-flat-button","","type","button","data-testid","check-key",3,"click"],["matInput","","type","password","data-testid","pw1","autocomplete","new-password",3,"input","value"],["matInput","","type","password","data-testid","pw2","autocomplete","new-password",3,"input","value"],["mat-flat-button","","type","button","data-testid","finish-pw",3,"click","disabled"],["mat-button","","type","button","data-testid","finish-nopw",3,"disabled"],["mat-button","","type","button","data-testid","finish-nopw",3,"click","disabled"],["mat-flat-button","","type","button","data-testid","to-app",1,"full",3,"click"]],template:function(t,r){if(t&1&&(p(0,"div",0),M(1,QT,21,0)(2,eA,19,4)(3,iA,13,4)(4,sA,19,3)(5,lA,21,7)(6,pA,22,10)(7,hA,11,0),m()),t&2){let o;y(),N((o=r.step())==="start"?1:o==="restore"?2:o==="passkey"?3:o==="key"?4:o==="confirm"?5:o==="password"?6:o==="done"?7:-1)}},dependencies:[Ct,xt,pn,Tn,mn,Sr,Cr,it,Qt],encapsulation:2})};var gA=(e,n)=>n.id;function bA(e,n){e&1&&(p(0,"p",2),b(1," Mit dem Wiederherstellungsschl\xFCssel entsperrt. Richte den Fingerabdruck beim n\xE4chsten Entsperren neu ein. "),m())}function yA(e,n){if(e&1){let t=fe();p(0,"div",5)(1,"div")(2,"div",12),b(3),m(),p(4,"div",4),b(5),m()(),p(6,"div",13),b(7),m(),p(8,"button",14),w("click",function(){let o=P(t).$implicit,i=x();return L(i.store.remove(o.id))}),oe(9,"app-icon",15),m()()}if(e&2){let t=n.$implicit,r=x();y(3),le(t.text),y(2),le(r.de(t.date)),y(),ce("pos",t.amount>0),y(),le(r.euro(t.amount)),y(),ge("aria-label","L\xF6schen: "+t.text),y(),R("size",20)}}function vA(e,n){e&1&&(p(0,"p",1),b(1,"Noch keine Testbuchungen."),m())}var fd=class e{vault=f(Re);store=f(Yt);euro=cd;de(n){return new Date(n+"T12:00:00").toLocaleDateString("de-DE",{weekday:"short",day:"2-digit",month:"2-digit",year:"numeric"})}static \u0275fac=function(t){return new(t||e)};static \u0275cmp=Q({type:e,selectors:[["app-start-page"]],decls:28,vars:2,consts:[[1,"page"],[1,"muted"],[1,"warn"],[1,"card"],[1,"muted","small"],[1,"row"],[1,"actions"],["mat-flat-button","","routerLink","/neu","data-testid","new-entry"],["name","plus"],[1,"small"],["mat-stroked-button","","routerLink","/pruefung"],["mat-button","","routerLink","/einstellungen"],[1,"t"],[1,"num","amt"],["mat-icon-button","","type","button",3,"click"],["name","trash",3,"size"]],template:function(t,r){t&1&&(p(0,"div",0)(1,"h1"),b(2,"Version 0.1: Machbarkeitstest"),m(),p(3,"p",1),b(4," Diese Version pr\xFCft, ob alles Wichtige auf deinem Handy funktioniert. Echte Buchungen kommen mit Version 0.2. "),m(),M(5,bA,2,0,"p",2),p(6,"div",3)(7,"h2"),b(8,"Testbuchungen"),m(),p(9,"p",4),b(10," Verschl\xFCsselt gespeichert. Erfasse zwei, drei Eintr\xE4ge und pr\xFCfe, ob sie nach dem Sperren, ohne Netz und im Backup wieder da sind. "),m(),Qo(11,yA,10,7,"div",5,gA,!1,vA,2,0,"p",1),p(14,"div",6)(15,"a",7),oe(16,"app-icon",8),b(17," Testbuchung erfassen"),m()()(),p(18,"div",3)(19,"h2"),b(20,"Dein Pr\xFCfauftrag"),m(),p(21,"p",9),b(22," Die Machbarkeitspr\xFCfung zeigt, was dein Handy kann, und hat eine Checkliste f\xFCr die Tests von Hand. Am Ende kopierst du das Ergebnis und schickst es mir. "),m(),p(23,"div",6)(24,"a",10),b(25,"Machbarkeitspr\xFCfung \xF6ffnen"),m(),p(26,"a",11),b(27,"Backup erstellen"),m()()()()),t&2&&(y(5),N(r.vault.unlockedWith()==="wiederherstellung"?5:-1),y(6),Xo(r.store.entries()))},dependencies:[Ct,xt,Ga,it,Qt],styles:[".row[_ngcontent-%COMP%]{display:grid;grid-template-columns:1fr auto auto;gap:8px;align-items:center;padding:6px 0;border-bottom:1px solid var(--%NS%hb-line)}.t[_ngcontent-%COMP%]{font-weight:600}.amt[_ngcontent-%COMP%]{font-weight:650}.pos[_ngcontent-%COMP%]{color:var(--%NS%hb-primary)}"]})};function _A(e,n){if(e&1){let t=fe();p(0,"div",1)(1,"button",6),w("click",function(){P(t);let o=x();return L(o.withPasskey())}),oe(2,"app-icon",7),b(3," Mit Fingerabdruck entsperren "),m()()}if(e&2){let t=x();y(),R("disabled",t.busy())}}function xA(e,n){if(e&1&&(p(0,"p",2),b(1),m()),e&2){let t=x();y(),le(t.error())}}function CA(e,n){if(e&1){let t=fe();p(0,"button",9),w("click",function(){P(t);let o=x(2);return L(o.mode.set("passwort"))}),b(1," Mit App-Passwort entsperren "),m()}}function SA(e,n){if(e&1){let t=fe();p(0,"mat-form-field",10)(1,"mat-label"),b(2,"App-Passwort"),m(),p(3,"input",11),w("input",function(o){P(t);let i=x(2);return L(i.pw=i.val(o))})("keyup.enter",function(){P(t);let o=x(2);return L(o.withPassword())}),m()(),p(4,"button",12),w("click",function(){P(t);let o=x(2);return L(o.withPassword())}),b(5),m()}if(e&2){let t=x(2);y(3),R("value",t.pw),y(),R("disabled",t.busy()),y(),ot(" ",t.busy()?"Einen Moment \u2026":"Entsperren"," ")}}function wA(e,n){if(e&1&&(p(0,"div",1),M(1,CA,2,0,"button",8)(2,SA,6,3),m()),e&2){let t=x();y(),N(t.mode()!=="passwort"?1:2)}}function EA(e,n){if(e&1){let t=fe();p(0,"p"),b(1,"Gib deinen Wiederherstellungsschl\xFCssel vom Zettel ein."),m(),p(2,"mat-form-field",10)(3,"mat-label"),b(4,"Wiederherstellungsschl\xFCssel"),m(),p(5,"input",13),w("input",function(o){P(t);let i=x(2);return L(i.key=i.val(o))}),m()(),p(6,"button",14),w("click",function(){P(t);let o=x(2);return L(o.withRecovery())}),b(7," Entsperren "),m()}if(e&2){let t=x(2);y(5),R("value",t.key),y(),R("disabled",t.busy())}}function DA(e,n){if(e&1){let t=fe();p(0,"p",15),b(1," Entsperrt. Willst du f\xFCr dieses Handy den Fingerabdruck neu einrichten? Danach entsperrst du wieder ohne Zettel. "),m(),p(2,"div",16)(3,"button",17),w("click",function(){P(t);let o=x(2);return L(o.newPasskey())}),oe(4,"app-icon",7),b(5," Fingerabdruck neu einrichten "),m(),p(6,"button",18),w("click",function(){P(t);let o=x(2);return L(o.done())}),b(7,"Sp\xE4ter"),m()()}if(e&2){let t=x(2);y(3),R("disabled",t.busy())}}function IA(e,n){if(e&1&&M(0,EA,8,2)(1,DA,8,1),e&2){let t=x();N(t.recovered()?1:0)}}function kA(e,n){if(e&1){let t=fe();p(0,"button",19),w("click",function(){P(t);let o=x();return L(o.mode.set("wiederherstellung"))}),b(1," Fingerabdruck klappt nicht? Wiederherstellungsschl\xFCssel verwenden "),m()}}var md=class e{vault=f(Re);router=f(He);route=f(cn);busy=E(!1);error=E("");mode=E("");recovered=E(!1);recoveredKey=null;pw="";key="";val(n){return n.target.value}async run(n,t=!0){this.busy.set(!0),this.error.set("");try{await n(),t&&this.done()}catch(r){this.error.set(r instanceof St||r instanceof Error?r.message:String(r))}finally{this.busy.set(!1)}}withPasskey(){return this.run(()=>this.vault.unlockWithPasskey())}withPassword(){return this.run(async()=>{await this.vault.unlockWithPassword(this.pw),this.pw=""})}withRecovery(){return this.run(async()=>{let n=Ql(this.key);if(!n)throw new Error("Der Schl\xFCssel hat 30 Zeichen in sechs Gruppen. Bitte nochmal vergleichen.");this.recoveredKey=await this.vault.unlockWithRecovery(n),this.key="",this.recovered.set(!0)},!1)}newPasskey(){return this.run(async()=>{this.recoveredKey&&await this.vault.replacePasskey(this.recoveredKey),this.recoveredKey=null})}done(){this.recoveredKey=null;let n=this.route.snapshot.queryParamMap.get("ziel");this.router.navigateByUrl(n&&n.startsWith("/")&&!n.startsWith("/entsperren")?n:"/start")}static \u0275fac=function(t){return new(t||e)};static \u0275cmp=Q({type:e,selectors:[["app-unlock-page"]],decls:12,vars:4,consts:[[1,"page"],[1,"card"],["role","alert",1,"bad"],["mat-button","","type","button","data-testid","show-recovery",1,"full"],[1,"small","muted"],["routerLink","/pruefung"],["mat-flat-button","","type","button","data-testid","unlock-passkey",1,"full","big",3,"click","disabled"],["name","fingerprint"],["mat-stroked-button","","type","button","data-testid","show-pw",1,"full"],["mat-stroked-button","","type","button","data-testid","show-pw",1,"full",3,"click"],["appearance","outline"],["matInput","","type","password","data-testid","unlock-pw","autocomplete","current-password",3,"input","keyup.enter","value"],["mat-flat-button","","type","button","data-testid","unlock-pw-go",3,"click","disabled"],["matInput","","data-testid","unlock-recovery","autocomplete","off","autocapitalize","characters","spellcheck","false",3,"input","value"],["mat-flat-button","","type","button","data-testid","unlock-recovery-go",3,"click","disabled"],[1,"hint"],[1,"actions"],["mat-flat-button","","type","button","data-testid","new-passkey",3,"click","disabled"],["mat-button","","type","button",3,"click"],["mat-button","","type","button","data-testid","show-recovery",1,"full",3,"click"]],template:function(t,r){t&1&&(p(0,"div",0)(1,"h1"),b(2,"Haushaltsbuch ist gesperrt"),m(),M(3,_A,4,1,"div",1),M(4,xA,2,1,"p",2),M(5,wA,3,1,"div",1),p(6,"div",1),M(7,IA,2,1)(8,kA,2,0,"button",3),m(),p(9,"p",4)(10,"a",5),b(11,"Machbarkeitspr\xFCfung ansehen"),m()()()),t&2&&(y(3),N(r.vault.meta()?.passkey?3:-1),y(),N(r.error()?4:-1),y(),N(r.vault.meta()?.password?5:-1),y(2),N(r.mode()==="wiederherstellung"?7:8))},dependencies:[Ct,xt,pn,Tn,mn,Sr,Cr,it,Qt],styles:[".big[_ngcontent-%COMP%]{min-height:64px;font-size:17px}"]})};var rh=async(e,n)=>{let t=f(Re),r=f(He),o=f(Yt);return await t.ready,t.status()==="neu"?r.parseUrl("/einrichtung"):t.status()!=="entsperrt"?r.createUrlTree(["/entsperren"],{queryParams:{ziel:n.url}}):(await o.load(),!0)},MA=async()=>{let e=f(Re),n=f(He);return await e.ready,e.status()==="neu"||e.status()==="entsperrt"?!0:n.parseUrl("/entsperren")},NA=async()=>{let e=f(Re),n=f(He);return await e.ready,e.status()==="neu"?n.parseUrl("/einrichtung"):!0},H0=[{path:"",pathMatch:"full",redirectTo:"start"},{path:"einrichtung",canActivate:[MA],component:ud,title:"Einrichtung"},{path:"entsperren",canActivate:[NA],component:md,title:"Entsperren"},{path:"start",canActivate:[rh],component:fd,title:"Haushaltsbuch"},{path:"neu",canActivate:[rh],component:ld,title:"Neue Buchung"},{path:"einstellungen",canActivate:[rh],component:dd,title:"Einstellungen"},{path:"pruefung",component:td,title:"Machbarkeitspr\xFCfung"},{path:"**",redirectTo:"start"}];var z0={providers:[Wu(),fp(H0,mp())]};var TA=6e4,pd=class e{vault=f(Re);store=f(Yt);router=f(He);zone=f(Y);hiddenAt=null;start(){document.addEventListener("visibilitychange",()=>this.zone.run(()=>document.hidden?this.leave():this.back())),window.addEventListener("pagehide",()=>this.leave())}leave(){document.body.classList.add("hb-covered"),this.hiddenAt??=Date.now()}back(){let n=this.hiddenAt===null?0:Date.now()-this.hiddenAt;this.hiddenAt=null,this.vault.status()==="entsperrt"&&n>=TA&&this.lockNow(this.router.url),document.body.classList.remove("hb-covered")}lockNow(n="/start"){this.vault.lock(),this.store.clear(),this.router.navigate(["/entsperren"],{queryParams:{ziel:n.startsWith("/entsperren")?"/start":n}})}static \u0275fac=function(t){return new(t||e)};static \u0275prov=G({token:e,factory:e.\u0275fac,providedIn:"root"})};var hd=class e{ready=E(!1);reg=null;async start(){if(Rv()||!("serviceWorker"in navigator))return;try{this.reg=await navigator.serviceWorker.register("sw.js")}catch{return}let n=this.reg,t=r=>{if(!r)return;let o=()=>{r.state==="installed"&&navigator.serviceWorker.controller&&this.ready.set(!0)};o(),r.addEventListener("statechange",o)};n.waiting&&navigator.serviceWorker.controller&&this.ready.set(!0),t(n.installing),n.addEventListener("updatefound",()=>t(n.installing)),setInterval(()=>{navigator.onLine&&n.update().catch(()=>{})},360*60*1e3)}apply(){let n=this.reg?.waiting;if(!n)return location.reload();navigator.serviceWorker.addEventListener("controllerchange",()=>location.reload(),{once:!0}),n.postMessage("SKIP_WAITING")}static \u0275fac=function(t){return new(t||e)};static \u0275prov=G({token:e,factory:e.\u0275fac,providedIn:"root"})};function AA(e,n){if(e&1){let t=fe();p(0,"header",0)(1,"a",2),b(2,"Haushaltsbuch"),m(),p(3,"span",3),b(4,"v0.1"),m(),p(5,"a",4),oe(6,"app-icon",5),m(),p(7,"button",6),w("click",function(){P(t);let o=x();return L(o.lock.lockNow("/start"))}),oe(8,"app-icon",7),m()()}}function RA(e,n){if(e&1){let t=fe();p(0,"div",1),b(1," Neue Version verf\xFCgbar. "),p(2,"button",8),w("click",function(){P(t);let o=x();return L(o.updates.apply())}),b(3,"Jetzt aktualisieren"),m()()}}var gd=class e{vault=f(Re);lock=f(pd);updates=f(hd);constructor(){this.lock.start(),this.updates.start()}static \u0275fac=function(t){return new(t||e)};static \u0275cmp=Q({type:e,selectors:[["app-root"]],decls:4,vars:2,consts:[[1,"bar"],["role","status",1,"update"],["routerLink","/start",1,"brand"],[1,"ver"],["mat-icon-button","","routerLink","/einstellungen","aria-label","Einstellungen"],["name","settings"],["mat-icon-button","","type","button","aria-label","Jetzt sperren",3,"click"],["name","lock"],["mat-flat-button","","type","button",3,"click"]],template:function(t,r){t&1&&(M(0,AA,9,0,"header",0),M(1,RA,4,0,"div",1),p(2,"main"),oe(3,"router-outlet"),m()),t&2&&(N(r.vault.status()==="entsperrt"?0:-1),y(),N(r.updates.ready()?1:-1))},dependencies:[Fa,it,Ct,xt,Ga,Qt],styles:[".bar[_ngcontent-%COMP%]{position:sticky;top:0;z-index:5;display:flex;align-items:center;gap:4px;padding:calc(env(safe-area-inset-top,0px) + 6px) 8px 6px 16px;background:var(--%NS%hb-bg);border-bottom:1px solid var(--%NS%hb-line)}.brand[_ngcontent-%COMP%]{font-weight:700;font-size:18px;color:var(--%NS%hb-ink);text-decoration:none}.ver[_ngcontent-%COMP%]{flex:1;color:var(--%NS%hb-muted);font-size:13px;margin-left:6px}.update[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;background:var(--%NS%hb-primary-soft);color:var(--%NS%hb-primary-ink);padding:8px 16px}"]})};var OA=new URLSearchParams(location.search).get("start");OA==="neue-buchung"&&history.replaceState(null,"",location.pathname+"#/neu");Lm(gd,z0).catch(e=>console.error(e));

/*! Lizenzhinweise der enthaltenen Bibliotheken

--------------------------------------------------------------------------------
Package: @angular/core
License: "MIT"

The MIT License

Copyright (c) 2010-2026 Google LLC. https://angular.dev/license

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

--------------------------------------------------------------------------------
Package: rxjs
License: "Apache-2.0"

                               Apache License
                         Version 2.0, January 2004
                      http://www.apache.org/licenses/

 TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION

 1. Definitions.

    "License" shall mean the terms and conditions for use, reproduction,
    and distribution as defined by Sections 1 through 9 of this document.

    "Licensor" shall mean the copyright owner or entity authorized by
    the copyright owner that is granting the License.

    "Legal Entity" shall mean the union of the acting entity and all
    other entities that control, are controlled by, or are under common
    control with that entity. For the purposes of this definition,
    "control" means (i) the power, direct or indirect, to cause the
    direction or management of such entity, whether by contract or
    otherwise, or (ii) ownership of fifty percent (50%) or more of the
    outstanding shares, or (iii) beneficial ownership of such entity.

    "You" (or "Your") shall mean an individual or Legal Entity
    exercising permissions granted by this License.

    "Source" form shall mean the preferred form for making modifications,
    including but not limited to software source code, documentation
    source, and configuration files.

    "Object" form shall mean any form resulting from mechanical
    transformation or translation of a Source form, including but
    not limited to compiled object code, generated documentation,
    and conversions to other media types.

    "Work" shall mean the work of authorship, whether in Source or
    Object form, made available under the License, as indicated by a
    copyright notice that is included in or attached to the work
    (an example is provided in the Appendix below).

    "Derivative Works" shall mean any work, whether in Source or Object
    form, that is based on (or derived from) the Work and for which the
    editorial revisions, annotations, elaborations, or other modifications
    represent, as a whole, an original work of authorship. For the purposes
    of this License, Derivative Works shall not include works that remain
    separable from, or merely link (or bind by name) to the interfaces of,
    the Work and Derivative Works thereof.

    "Contribution" shall mean any work of authorship, including
    the original version of the Work and any modifications or additions
    to that Work or Derivative Works thereof, that is intentionally
    submitted to Licensor for inclusion in the Work by the copyright owner
    or by an individual or Legal Entity authorized to submit on behalf of
    the copyright owner. For the purposes of this definition, "submitted"
    means any form of electronic, verbal, or written communication sent
    to the Licensor or its representatives, including but not limited to
    communication on electronic mailing lists, source code control systems,
    and issue tracking systems that are managed by, or on behalf of, the
    Licensor for the purpose of discussing and improving the Work, but
    excluding communication that is conspicuously marked or otherwise
    designated in writing by the copyright owner as "Not a Contribution."

    "Contributor" shall mean Licensor and any individual or Legal Entity
    on behalf of whom a Contribution has been received by Licensor and
    subsequently incorporated within the Work.

 2. Grant of Copyright License. Subject to the terms and conditions of
    this License, each Contributor hereby grants to You a perpetual,
    worldwide, non-exclusive, no-charge, royalty-free, irrevocable
    copyright license to reproduce, prepare Derivative Works of,
    publicly display, publicly perform, sublicense, and distribute the
    Work and such Derivative Works in Source or Object form.

 3. Grant of Patent License. Subject to the terms and conditions of
    this License, each Contributor hereby grants to You a perpetual,
    worldwide, non-exclusive, no-charge, royalty-free, irrevocable
    (except as stated in this section) patent license to make, have made,
    use, offer to sell, sell, import, and otherwise transfer the Work,
    where such license applies only to those patent claims licensable
    by such Contributor that are necessarily infringed by their
    Contribution(s) alone or by combination of their Contribution(s)
    with the Work to which such Contribution(s) was submitted. If You
    institute patent litigation against any entity (including a
    cross-claim or counterclaim in a lawsuit) alleging that the Work
    or a Contribution incorporated within the Work constitutes direct
    or contributory patent infringement, then any patent licenses
    granted to You under this License for that Work shall terminate
    as of the date such litigation is filed.

 4. Redistribution. You may reproduce and distribute copies of the
    Work or Derivative Works thereof in any medium, with or without
    modifications, and in Source or Object form, provided that You
    meet the following conditions:

    (a) You must give any other recipients of the Work or
        Derivative Works a copy of this License; and

    (b) You must cause any modified files to carry prominent notices
        stating that You changed the files; and

    (c) You must retain, in the Source form of any Derivative Works
        that You distribute, all copyright, patent, trademark, and
        attribution notices from the Source form of the Work,
        excluding those notices that do not pertain to any part of
        the Derivative Works; and

    (d) If the Work includes a "NOTICE" text file as part of its
        distribution, then any Derivative Works that You distribute must
        include a readable copy of the attribution notices contained
        within such NOTICE file, excluding those notices that do not
        pertain to any part of the Derivative Works, in at least one
        of the following places: within a NOTICE text file distributed
        as part of the Derivative Works; within the Source form or
        documentation, if provided along with the Derivative Works; or,
        within a display generated by the Derivative Works, if and
        wherever such third-party notices normally appear. The contents
        of the NOTICE file are for informational purposes only and
        do not modify the License. You may add Your own attribution
        notices within Derivative Works that You distribute, alongside
        or as an addendum to the NOTICE text from the Work, provided
        that such additional attribution notices cannot be construed
        as modifying the License.

    You may add Your own copyright statement to Your modifications and
    may provide additional or different license terms and conditions
    for use, reproduction, or distribution of Your modifications, or
    for any such Derivative Works as a whole, provided Your use,
    reproduction, and distribution of the Work otherwise complies with
    the conditions stated in this License.

 5. Submission of Contributions. Unless You explicitly state otherwise,
    any Contribution intentionally submitted for inclusion in the Work
    by You to the Licensor shall be under the terms and conditions of
    this License, without any additional terms or conditions.
    Notwithstanding the above, nothing herein shall supersede or modify
    the terms of any separate license agreement you may have executed
    with Licensor regarding such Contributions.

 6. Trademarks. This License does not grant permission to use the trade
    names, trademarks, service marks, or product names of the Licensor,
    except as required for reasonable and customary use in describing the
    origin of the Work and reproducing the content of the NOTICE file.

 7. Disclaimer of Warranty. Unless required by applicable law or
    agreed to in writing, Licensor provides the Work (and each
    Contributor provides its Contributions) on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
    implied, including, without limitation, any warranties or conditions
    of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A
    PARTICULAR PURPOSE. You are solely responsible for determining the
    appropriateness of using or redistributing the Work and assume any
    risks associated with Your exercise of permissions under this License.

 8. Limitation of Liability. In no event and under no legal theory,
    whether in tort (including negligence), contract, or otherwise,
    unless required by applicable law (such as deliberate and grossly
    negligent acts) or agreed to in writing, shall any Contributor be
    liable to You for damages, including any direct, indirect, special,
    incidental, or consequential damages of any character arising as a
    result of this License or out of the use or inability to use the
    Work (including but not limited to damages for loss of goodwill,
    work stoppage, computer failure or malfunction, or any and all
    other commercial damages or losses), even if such Contributor
    has been advised of the possibility of such damages.

 9. Accepting Warranty or Additional Liability. While redistributing
    the Work or Derivative Works thereof, You may choose to offer,
    and charge a fee for, acceptance of support, warranty, indemnity,
    or other liability obligations and/or rights consistent with this
    License. However, in accepting such obligations, You may act only
    on Your own behalf and on Your sole responsibility, not on behalf
    of any other Contributor, and only if You agree to indemnify,
    defend, and hold each Contributor harmless for any liability
    incurred by, or claims asserted against, such Contributor by reason
    of your accepting any such warranty or additional liability.

 END OF TERMS AND CONDITIONS

 APPENDIX: How to apply the Apache License to your work.

    To apply the Apache License to your work, attach the following
    boilerplate notice, with the fields enclosed by brackets "[]"
    replaced with your own identifying information. (Don't include
    the brackets!)  The text should be enclosed in the appropriate
    comment syntax for the file format. We also recommend that a
    file or class name and description of purpose be included on the
    same "printed page" as the copyright notice for easier
    identification within third-party archives.

 Copyright (c) 2015-2018 Google, Inc., Netflix, Inc., Microsoft Corp. and contributors

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 

--------------------------------------------------------------------------------
Package: tslib
License: "0BSD"

Copyright (c) Microsoft Corporation.

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
PERFORMANCE OF THIS SOFTWARE.
--------------------------------------------------------------------------------
Package: @angular/common
License: "MIT"

The MIT License

Copyright (c) 2010-2026 Google LLC. https://angular.dev/license

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

--------------------------------------------------------------------------------
Package: @angular/platform-browser
License: "MIT"

The MIT License

Copyright (c) 2010-2026 Google LLC. https://angular.dev/license

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

--------------------------------------------------------------------------------
Package: @angular/router
License: "MIT"

The MIT License

Copyright (c) 2010-2026 Google LLC. https://angular.dev/license

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

--------------------------------------------------------------------------------
Package: @angular/cdk
License: "MIT"

The MIT License

Copyright (c) 2026 Google LLC.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

--------------------------------------------------------------------------------
Package: @angular/material
License: "MIT"

The MIT License

Copyright (c) 2026 Google LLC.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

--------------------------------------------------------------------------------
Package: @angular/forms
License: "MIT"

The MIT License

Copyright (c) 2010-2026 Google LLC. https://angular.dev/license

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

--------------------------------------------------------------------------------
Package: idb
License: "ISC"

ISC License (ISC)
Copyright (c) 2016, Jake Archibald <jaffathecake@gmail.com>

Permission to use, copy, modify, and/or distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

--------------------------------------------------------------------------------

*/
