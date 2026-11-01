# v4shim.py — 引擎嵌入v4包装件(VPS部署版·钥匙读同目录maas.key·npz可选)
# [M6-2·20261025·integ] 零向量兜底显性化: 模块级计数器ZERO_EMBED_COUNT——encode()每次
#   兜底零向量时+1, 每累计100次打一行WARN日志(v4静默零向量=无声退化, 必须显性化);
#   /health可选字段embed_zero_count消费本计数器。计数fail-soft绝不影响encode语义
#   (v4本身仍不默认启用·dev用MiniLM)。
import json, hashlib, os, threading, time, logging, urllib.request, numpy as np
API='https://dashscope.aliyuncs.com/compatible-mode/v1/embeddings'
KEY=open(os.path.join(os.path.dirname(os.path.abspath(__file__)),'maas.key')).read().strip()
MODEL='text-embedding-v4'; DIM=1024

# ── [M6-2] 零向量兜底监控(模块级·/health读) ──────────────────────────────────
ZERO_EMBED_COUNT=0
_ZERO_LOG=logging.getLogger("v4shim")
def _bump_zero(n=1):
    """零向量兜底计数+每100次WARN(跨100整数倍即报·fail-soft绝不抛)。"""
    global ZERO_EMBED_COUNT
    try:
        before=ZERO_EMBED_COUNT
        ZERO_EMBED_COUNT=before+int(n)
        if ZERO_EMBED_COUNT//100 > before//100:
            _ZERO_LOG.warning("v4 zero-embed fallback total=%d (silent degradation watch)",
                              ZERO_EMBED_COUNT)
    except Exception:
        pass
class V4Model:
    def __init__(self, npz_path=None):
        self.cache={}
        if npz_path and os.path.exists(npz_path):
            z=np.load(npz_path, allow_pickle=True)
            self.cache={k: v for k, v in zip(list(z['keys']), list(z['vecs']))}
        self._lk=threading.Lock()
        print(f"[v4shim] 就绪 缓存{len(self.cache)}条 text-embedding-v4/{DIM}维", flush=True)
    @staticmethod
    def _h(t): return hashlib.md5(t.encode()).hexdigest()
    def _api(self, batch):
        body=json.dumps({"model":MODEL,"input":batch,"dimensions":DIM,"encoding_format":"float"}).encode()
        last=""
        for att in range(5):
            try:
                req=urllib.request.Request(API,data=body,headers={'Content-Type':'application/json','Authorization':f'Bearer {KEY}'})
                with urllib.request.urlopen(req,timeout=120) as r:
                    d=json.loads(r.read().decode())
                out=[None]*len(batch)
                for item in d['data']:
                    v=np.array(item['embedding'],dtype=np.float32); n=np.linalg.norm(v)
                    out[item['index']]=v/n if n>0 else v
                return out
            except Exception as e:
                last=str(e)[:80]; time.sleep(2*(att+1))
        raise RuntimeError(f"v4 api 5attempts: {last}")
    def encode(self, texts, batch_size=64, show_progress_bar=False, normalize_embeddings=True, **kw):
        # [防500·20261025]: 任何异常/缺条/空文本→零向量兜底·本函数绝不抛
        # [M6-2]: 每次兜底零向量ZERO_EMBED_COUNT+1(空文本/API失败/缺条/残留None/整批异常)
        try:
            texts=[str(t or "") for t in texts]
            out=[None]*len(texts); miss=[]
            for i,t in enumerate(texts):
                if not t.strip():
                    out[i]=np.zeros(DIM,dtype=np.float32); _bump_zero()  # 空文本零向量·不调API
                    continue
                v=self.cache.get(self._h(t))
                if v is not None: out[i]=v
                else: miss.append(i)
            B=10
            for j in range(0,len(miss),B):
                idxs=miss[j:j+B]; batch=[texts[i] for i in idxs]
                try:
                    vecs=self._api(batch)
                except Exception:
                    vecs=[np.zeros(DIM,dtype=np.float32) for _ in batch]
                    _bump_zero(len(batch))  # API失败整批兜底
                for k,i in enumerate(idxs):
                    if k < len(vecs) and vecs[k] is not None:
                        out[i]=vecs[k]
                        # [M7·dev_h修复20261025·encode memo] 成功向量回填md5缓存: 实测(py-spy
                        # 21:47实弹)k4搜索包装走organ全库legacy路径, _emb_scores逐conv调
                        # qmodel.encode → v4下=每搜索1007次串行API(5-8分钟)→T1必炸→100%降级
                        # 词面快车道。MiniLM时代同路径靠k4 memo(500ms TTL)命中归零, v4对称化:
                        # 同文本第二次起走内存dict(0.1ms)→1007次塌缩为1次真实API。进程内缓存,
                        # 无TTL(评测/单池场景内存≈条数×4KB·生产建议接assemble_v4cache预热的
                        # npz或加LRU)。API失败/零向量不入缓存(防毒化)。
                        if vecs[k].any():
                            self.cache[self._h(texts[i])]=vecs[k]
                # [硬保证] API缺条(内容安全拦截等)→缺位零向量·绝无None残留
                for i in idxs:
                    if out[i] is None:
                        out[i]=np.zeros(DIM,dtype=np.float32); _bump_zero()
            for i,v in enumerate(out):
                if v is None:
                    out[i]=np.zeros(DIM,dtype=np.float32); _bump_zero()
            return np.array(out,dtype=np.float32)
        except Exception:
            _bump_zero(len(texts))  # 整批异常兜底
            return np.zeros((len(texts),DIM),dtype=np.float32)
def install(svc, npz_path=None):
    if os.environ.get('BRAINOS_EMBED_PROVIDER','')!='dashscope_v4':
        return False
    svc.organ._qmodel=V4Model(npz_path)
    # [M7·dev_h修复20261025·按维度保留向量] 原实现无条件 _emb={} 清空: boot相 startup-heal
    # 刚从 emb_cache.npz 恢复的 v4 向量(1024维·shutdown相原子落盘)也被抹掉→每次重启全池
    # 走 API 重嵌(实测3675卡≈35分钟+费用)。改按维度甄别: 保留 len==DIM(本shim产)的条目,
    # 清除 384 维 MiniLM 残留——嵌入-模型同源由维度保证, 合规语义不变。
    _emb0 = getattr(svc.organ, '_emb', None)
    if isinstance(_emb0, dict) and _emb0:
        _kept = {k: v for k, v in _emb0.items()
                 if hasattr(v, 'shape') and len(v) == DIM}
        _dropped = len(_emb0) - len(_kept)
        svc.organ._emb = _kept
        if _dropped or len(_kept):
            print(f'[v4shim] _emb 按维度甄别: 保留{len(_kept)}条({DIM}维) 清除{_dropped}条(非{DIM}维)', flush=True)
    else:
        svc.organ._emb = {}
    # [M7·dev_h修复20261025·缺向量必v4重建] 清残留后凡缺向量之卡一律标脏→finalize worker
    # 用本shim的V4Model重嵌(若不标脏, worker无脏集可依据, 语义通道将一直空转到词面)。
    _cards = getattr(svc.organ, '_cards', None) or {}
    _emb2 = getattr(svc.organ, '_emb', None) or {}
    _miss = [k for k in _cards if k not in _emb2]
    if _miss:
        _convs = {str(_cards[k].get('conv')) for k in _miss}
        svc.organ._dirty_convs = getattr(svc.organ, '_dirty_convs', set()) | _convs
        svc.organ._dirty_keys = getattr(svc.organ, '_dirty_keys', set()) | set(_miss)
        print(f'[v4shim] 缺向量{len(_miss)}条(conv {len(_convs)}) 标脏待worker v4重嵌', flush=True)
    if hasattr(svc.organ,'use_embed'): svc.organ.use_embed=True
    print('[v4shim] 已安装: organ._qmodel=V4Model (text-embedding-v4·合规嵌入)', flush=True)
    return True
