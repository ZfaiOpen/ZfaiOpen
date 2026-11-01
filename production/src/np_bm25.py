# np_bm25.py — [刀8.13·BM25 numpy化 20261018] rank_bm25.BM25Okapi逐位等价numpy实现
# 病根: get_scores的per-doc纯Python循环 O(查询词×全库文档)持GIL 2-3s/笔@18万卡
#      =FULL期add/search全堵的GIL第一大户。
# 修法: __init__预构建doc×term CSR矩阵+idf向量; get_scores=CSR列切片(C速度)
#      →逐位同公式(求和顺序差<1e-15·排序不变)。
# 接口: __init__(corpus)+get_scores(query)=与BM25Okapi drop-in等价。
# 回退: scipy缺失时调用方回退原BM25Okapi。
from __future__ import annotations
import math
import numpy as np
from scipy.sparse import csr_matrix


class NumpyBM25Okapi:
    """BM25Okapi逐位等价numpy实现(k1=1.5/b=0.75/epsilon=0.25·ATIRE idf+eps地板)。"""

    def __init__(self, corpus, k1: float = 1.5, b: float = 0.75, epsilon: float = 0.25):
        self.corpus_size = len(corpus)
        self.k1, self.b, self.epsilon = k1, b, epsilon
        self.doc_len = np.array([len(d) for d in corpus], dtype=np.float64)
        avgdl = float(self.doc_len.mean()) if self.corpus_size else 1.0
        self.doc_freqs: list[dict] = []
        vocab: dict[str, int] = {}
        rows: list[int] = []
        cols: list[int] = []
        vals: list[float] = []
        nd: dict[str, int] = {}
        for di, doc in enumerate(corpus):
            freq: dict[str, int] = {}
            for t in doc:
                c = vocab.setdefault(t, len(vocab))
                rows.append(di)
                cols.append(c)
                vals.append(1.0)
                freq[t] = freq.get(t, 0) + 1
            self.doc_freqs.append(freq)
            for t in freq:
                nd[t] = nd.get(t, 0) + 1
        self._vocab = vocab
        self._tm = csr_matrix((np.array(vals), (np.array(rows), np.array(cols))),
                              shape=(self.corpus_size, len(vocab)))
        # idf=BM25Okapi._calc_idf逐位同式(log差·负idf→eps*average_idf地板)
        self.idf: dict[str, float] = {}
        idf_sum = 0.0
        negative: list[str] = []
        for word, freq in nd.items():
            idf = math.log(self.corpus_size - freq + 0.5) - math.log(freq + 0.5)
            self.idf[word] = idf
            idf_sum += idf
            if idf < 0:
                negative.append(word)
        self.average_idf = idf_sum / len(self.idf) if self.idf else 0.0
        eps = self.epsilon * self.average_idf
        for word in negative:
            self.idf[word] = eps
        self._idf_vec = np.zeros(len(vocab))
        for t, c in vocab.items():
            self._idf_vec[c] = self.idf.get(t, 0.0)
        # 预计算分母(每文档常数项): k1*(1-b+b*len/avgdl)
        self._denom = self.k1 * (1 - self.b + self.b * self.doc_len / avgdl)

    def get_scores(self, query):
        score = np.zeros(self.corpus_size)
        cols: list[int] = []
        seen: set[int] = set()
        for t in query:
            c = self._vocab.get(t)
            if c is not None and c not in seen:
                cols.append(c)
                seen.add(c)
        if not cols:
            return score
        qf = np.asarray(self._tm[:, cols].todense())
        for i, c in enumerate(cols):
            score += self._idf_vec[c] * (qf[:, i] * (self.k1 + 1) /
                                         (qf[:, i] + self._denom))
        return score
