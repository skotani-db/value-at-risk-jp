from pyspark.sql.functions import udf
from pyspark.ml.linalg import Vectors, VectorUDT


@udf("double")
def compute_return(first, close):
    """対数リターンを計算"""
    import numpy as np
    return float(np.log(close / first))


@udf("float")
def wsse_udf(p, a):
    """加重二乗誤差の合計を計算"""
    return float((p - a) ** 2)


@udf('float')
def get_var_udf(simulations, var):
    """バリュー・アット・リスクを計算"""
    from utils.var_utils import get_var
    return get_var(simulations, var)


@udf('int')
def count_breaches(xs, var):
    """閾値超過回数をカウントし、バーゼルゾーンを返す"""
    breaches = len([x for x in xs if x <= var])
    if breaches <= 3:
        return 0
    elif breaches < 10:
        return 1
    else:
        return 2


@udf('float')
def get_shortfall_udf(simulations, var):
    """期待ショートフォールを計算"""
    from utils.var_utils import get_shortfall
    return get_shortfall(simulations, var)


@udf(VectorUDT())
def weighted_returns(returns, weight):
    """加重リターンベクトルを計算"""
    return Vectors.dense(returns.toArray() * weight)


@udf('array<double>')
def compute_avg(xs):
    """マーケット指標の平均を計算"""
    import numpy as np
    mean = np.array(xs).mean(axis=0)
    return mean.tolist()


@udf('array<array<double>>')
def compute_cov(xs):
    """マーケット指標の共分散行列を計算"""
    import pandas as pd
    return pd.DataFrame(xs).cov().values.tolist()


# 共分散行列とマーケット指標の平均が与えられた場合、多変量分布からサンプリング
# 再現性のためにシードを渡すことが可能
# 多くのデータサイエンティストはnp.random.seed(seed)でシードを追加するが、
# 分散処理の性質とグローバルにシードを設定する副作用を考慮する必要がある
# 代わりに rng = np.random.default_rng(seed) を使用する

@udf('array<float>')
def simulate_market(vol_avg, vol_cov, seed):
    """多変量正規分布から市場条件をシミュレーション"""
    import numpy as np
    rng = np.random.default_rng(seed)
    return rng.multivariate_normal(vol_avg, vol_cov).tolist()
