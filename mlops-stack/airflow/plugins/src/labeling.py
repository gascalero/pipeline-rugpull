import pandas as pd
import numpy as np
BLOCKSTUDY = 13220488

#last_transfer = df_token_transfers.groupby('token_address')['block_number'].max()
#inactive_transfers = (BLOCKSTUDY - last_transfer > 160000).astype(int)

def compute_drawdown(series):
    """Caída máxima relativa desde el pico hasta el valle"""
    running_max = np.maximum.accumulate(series)
    valley_idx = np.argmax(running_max - series)
    peak_idx = np.argmax(series[:valley_idx]) if valley_idx > 0 else 0
    
    peak_val = series[peak_idx]
    valley_val = series[valley_idx]
    
    if peak_val == 0:
        return 0, peak_idx, valley_idx
    
    mdd = (valley_val - peak_val) / peak_val
    return mdd, peak_idx, valley_idx


def compute_recovery(series, peak_idx, valley_idx):
    """Cuánto se recuperó la serie después del valle"""
    peak_val = series[peak_idx]
    valley_val = series[valley_idx]
    drop = peak_val - valley_val
    
    if drop == 0:
        return 0
    
    return (series[-1] - valley_val) / drop


def build_reserve_series(syncs, weth_position, token_decimals):
    """Construye series de liquidez y precio desde eventos Sync"""
    token_position = 1 - weth_position
    liquidity, prices, blocks = [], [], []
    
    for _, row in syncs.iterrows():
        weth_r = int(row[f'amount{weth_position}_or_reserve{weth_position}_hex'], 16) / 10**18
        token_r = int(row[f'amount{token_position}_or_reserve{token_position}_hex'], 16) / 10**token_decimals
        
        if token_r == 0 or weth_r == 0:
            continue
        
        blocks.append(row['block_number'])
        liquidity.append(weth_r * token_r)
        prices.append(weth_r / token_r)
    
    return np.array(blocks), np.array(liquidity), np.array(prices)


def extract_pool_features(token_address, df_pools, df_pool_events, blockstudy):
    
    pool_info = df_pools[df_pools['token_address'] == token_address].iloc[0]
    pair_address = pool_info['pair_address']
    weth_position = 0 if pool_info['weth_is_token0'] else 1
    token_position = 1 - weth_position
    token_decimals = pool_info[f'token{token_position}_decimals']
    
    syncs = df_pool_events[
        (df_pool_events['pair_address'] == pair_address) &
        (df_pool_events['event_type'] == 'sync')
    ].sort_values('block_number')
    
    if len(syncs) < 5:
        return None
    
    blocks, liquidity, prices = build_reserve_series(syncs, weth_position, token_decimals)
    
    if len(blocks) < 5:
        return None
    
    liq_mdd, liq_peak, liq_valley = compute_drawdown(liquidity)
    liq_rc = compute_recovery(liquidity, liq_peak, liq_valley)
    
    price_mdd, price_peak, price_valley = compute_drawdown(prices)
    price_rc = compute_recovery(prices, price_peak, price_valley)
    
    return {
        'token_address': token_address,
        'pair_address': pair_address,
        'inactive': int(blockstudy - blocks[-1] > 160000),
        'late_creation': int(blockstudy - blocks[0] < 160000),
        'liq_mdd': liq_mdd,
        'liq_rc': liq_rc,
        'price_mdd': price_mdd,
        'price_rc': price_rc,
        'total_syncs': len(blocks)
    }

def assign_fraud_labels(df_pool_features, inactive_transfers_series):
    
    working_df = df_pool_features.copy()
    
    # Combinar señal de inactividad: pool Y transfers deben estar inactivos
    working_df['transfer_inactive'] = inactive_transfers_series
    working_df['fully_inactive'] = (working_df['inactive'] == 1) & (working_df['transfer_inactive'] == 1)
    
    # Solo tokens con suficiente historia y completamente abandonados
    eligible = working_df[
        (working_df['fully_inactive'] == True) & 
        (working_df['late_creation'] == 0)
    ]
    
    records = []
    
    # Esquema 1: vaciado total de liquidez sin recuperación
    liquidity_stolen = eligible[
        (eligible['liq_mdd'] == -1.0) &
        (eligible['liq_rc'] <= 0.2)
    ]
    for token in liquidity_stolen.index:
        records.append({
            'token_address': token,
            'pair_address': working_df.loc[token, 'pair_address'],
            'label': 0,
            'fraud_type': 'liquidity_stealing'
        })
    
    # Esquema 2: colapso de precio sin movimiento de liquidez
    dumping = eligible[
        (eligible['liq_mdd'] == 0) &
        (eligible['price_mdd'].between(-1.0, -0.9)) &
        (eligible['price_rc'].between(0, 0.01))
    ]
    for token in dumping.index:
        records.append({
            'token_address': token,
            'pair_address': working_df.loc[token, 'pair_address'],
            'label': 0,
            'fraud_type': 'dumping'
        })
    

        # Tokens no etiquetados como fraude → legítimos (label=1)
    fraud_tokens = set([r['token_address'] for r in records])
    for token in df_pool_features.index:
        if token not in fraud_tokens:
            records.append({
                'token_address': token,
                'pair_address': working_df.loc[token, 'pair_address'],
                'label': 1,
                'fraud_type': 'legitimate'
            })

    return pd.DataFrame(records)

def build_label_dataframe(df_pools, df_pool_events, df_token_transfers, blockstudy):
    
    # Features intermedios para labeling
    label_features_list = []
    for token in df_pools['token_address']:
        result = extract_pool_features(token, df_pools, df_pool_events, blockstudy)
        if result:
            label_features_list.append(result)
    
    df_label_features = pd.DataFrame(label_features_list).set_index('token_address')
    
    # Inactividad transfers
    inactive_transfers = (blockstudy - df_token_transfers.groupby('token_address')['block_number'].max() > 160000).astype(int)
    
    # Asignar labels
    df_labels = assign_fraud_labels(df_label_features, inactive_transfers)
    
    return df_labels

