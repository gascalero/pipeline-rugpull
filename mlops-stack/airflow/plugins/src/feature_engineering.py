import numpy as np
from collections import defaultdict

ETH_ADDRESS = "0x0000000000000000000000000000000000000000"
DEAD_ADDRESS = "0x000000000000000000000000000000000000dead"

WINDOW = 6646  # ~24h en bloques




def get_pool_features(token_address, df_pools, df_pool_events, eval_block):
    
    pool_info = df_pools[df_pools['token_address'] == token_address].iloc[0]
    pair_address = pool_info['pair_address']
    weth_is_token0 = pool_info['weth_is_token0']
    token_decimals = pool_info['token1_decimals'] if weth_is_token0 else pool_info['token0_decimals']

    syncs = df_pool_events[
        (df_pool_events['pair_address'] == pair_address) &
        (df_pool_events['event_type'] == 'sync') &
        (df_pool_events['block_number'] < eval_block)
    ].copy()

    # Guard: sin actividad en la ventana
    if len(syncs) == 0:
        return {
            'n_syncs': 0,
            'WETH': 0,
            'prices': 0,
            'liquidity': 0,
        }

    syncs['reserve0'] = syncs['amount0_or_reserve0_hex'].apply(lambda x: int(x, 16))
    syncs['reserve1'] = syncs['amount1_or_reserve1_hex'].apply(lambda x: int(x, 16))

    if weth_is_token0:
        WETH = syncs['reserve0'] / 10**18
        TOKEN = syncs['reserve1'] / 10**token_decimals
    else:
        WETH = syncs['reserve1'] / 10**18
        TOKEN = syncs['reserve0'] / 10**token_decimals

    # Guard: evitar división por cero
    TOKEN = TOKEN.replace(0, float('nan'))
    PRICES = WETH / TOKEN
    LIQUIDITY = WETH * TOKEN

    return {
        'n_syncs': len(syncs),
        'WETH': WETH.iloc[-1],
        'prices': PRICES.iloc[-1],
        'liquidity': LIQUIDITY.iloc[-1],
    }

def distribution_metric(balances, total_supply):
    """Calcula HHI dado un diccionario de balances y el supply total"""
    return sum(
        (value / total_supply) ** 2
        for holder, value in balances.items()
        if holder not in [ETH_ADDRESS, DEAD_ADDRESS]
    )

def get_curve(token_address, df_token_transfers, eval_block):
    """Calcula HHI de distribución del token"""
    transfers = df_token_transfers[
        (df_token_transfers['token_address'] == token_address) &
        (df_token_transfers['block_number'] < eval_block)
    ].sort_values('block_number')

    balances = defaultdict(lambda: 0)
    total_supply = 0

    for _, row in transfers.iterrows():
        from_ = row['from_address']
        to_ = row['to_address']
        value = float(row['value'])

        balances[from_] -= value
        balances[to_] += value

        if from_ == ETH_ADDRESS:
            total_supply += value
            balances[from_] = 0
        if to_ == ETH_ADDRESS:
            total_supply -= value
            balances[to_] = 0

    if total_supply != 0:
        curve = distribution_metric(balances, total_supply)
    else:
        curve = 1

    return {'tx_curve': curve}

def get_transfer_features(token_address, df_token_transfers, eval_block):
    """Calcula features de transfers del token"""
    transfers = df_token_transfers[
        (df_token_transfers['token_address'] == token_address) &
        (df_token_transfers['block_number'] < eval_block)
    ].copy()

    n_unique_addresses = len(
        set(transfers['from_address'].tolist() + transfers['to_address'].tolist())
    )

    return {
        'num_transactions': len(transfers),
        'n_unique_addresses': n_unique_addresses,
    }

def get_lp_features(token_address, eval_block, df_pools, df_pool_events, df_tokens_metadata):
    
    pool_info = df_pools[df_pools['token_address'] == token_address].iloc[0]
    pair_address = pool_info['pair_address']
    pool_creation_block = pool_info['block_number']
    token_creation_block = df_tokens_metadata[
        df_tokens_metadata['token_address'] == token_address
    ]['token_creation_block'].iloc[0]

    lp_events = df_pool_events[
        (df_pool_events['pair_address'] == pair_address) &
        (df_pool_events['block_number'] < eval_block)
    ]

    return {
        'mints': len(lp_events[lp_events['event_type'] == 'mint']),
        'burns': len(lp_events[lp_events['event_type'] == 'burn']),
        'difference_token_pool': pool_creation_block - token_creation_block
    }

'====== Main function to compute all features ======'

def compute_features(token_address, df_pools, df_pool_events, df_token_transfers, df_tokens_metadata):
    """Función principal: computa todos los features para un token dado"""
    pool_info = df_pools[df_pools['token_address'] == token_address].iloc[0]
    first_sync_block = df_pool_events[
        (df_pool_events['pair_address'] == pool_info['pair_address']) &
        (df_pool_events['event_type'] == 'sync')
    ]['block_number'].min()

    eval_block = first_sync_block + WINDOW

    features = {'token_address': token_address}  # ← clave para merge posterior
    features.update(get_pool_features(token_address, df_pools, df_pool_events, eval_block))
    features.update(get_transfer_features(token_address, df_token_transfers, eval_block))
    features.update(get_curve(token_address, df_token_transfers, eval_block))
    features.update(get_lp_features(token_address, eval_block, df_pools, df_pool_events, df_tokens_metadata))

    return features