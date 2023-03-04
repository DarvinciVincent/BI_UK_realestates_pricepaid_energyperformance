import pandas as pd
import warnings

# Disable (false positive) warning
pd.options.mode.chained_assignment = None  # default='warn'
warnings.filterwarnings("ignore", 'This pattern has match groups')

def normalize_address_variables(df):
    # 1 Remove leading and trailing whitespace characters
    # 2 Capitalize address variables
    df['add1'] = df['address1'].str.strip().str.upper()
    df['add2'] = df['address2'].str.strip().str.upper()
    df['add3'] = df['address3'].str.strip().str.upper()
    df['add'] = df['address'].str.strip().str.upper()
    df = df.fillna('')
    return df


# Define functions for matching process
def filter_before_matching(ppd_temp, epc_temp, filter_requirements, rule):
    # Check filter requirements
    if rule in filter_requirements:
        if 'ppd' in filter_requirements[rule]:
            filter_rules = filter_requirements[rule]['ppd']
            for filter in filter_rules:
                if filter[1] == 'equal':
                    ppd_temp = ppd_temp[ppd_temp[filter[0]] == filter[2]]
                elif filter[1] == 'notequal':
                    ppd_temp = ppd_temp[ppd_temp[filter[0]] != filter[2]]
                elif filter[1] == 'innotregex':
                    ppd_temp = ppd_temp[ppd_temp[filter[2]].str.contains(filter[0], regex=False)]
                elif filter[1] == 'inregex':
                    ppd_temp = ppd_temp[ppd_temp[filter[2]].str.contains(filter[0], regex=True)]
                    
        if 'epc' in filter_requirements[rule]:
            filter_rules = filter_requirements[rule]['epc']
            for filter in filter_rules:
                if filter[1] == 'equal':
                    epc_temp = epc_temp[epc_temp[filter[0]] == filter[2]]
                elif filter[1] == 'notequal':
                    epc_temp = epc_temp[epc_temp[filter[0]] != filter[2]]
                elif filter[1] == 'innotregex':
                    epc_temp = epc_temp[epc_temp[filter[2]].str.contains(filter[0], regex=False)]
                elif filter[1] == 'inregex':
                    epc_temp = epc_temp[epc_temp[filter[2]].str.contains(filter[0], regex=True)]
                elif filter[1] == 'notinnotregex':
                    epc_temp = epc_temp[~(epc_temp[filter[2]].str.contains(filter[0], regex=False))]
                elif filter[1] == 'notinregex':
                    epc_temp = epc_temp[~(epc_temp[filter[2]].str.contains(filter[0], regex=True))]
    
    return ppd_temp, epc_temp

def match_keys(ppd, epc, matching_rules, filter_requirements):
    
    # Fill NAN, None
    ppd = ppd.fillna("")
    epc = epc.fillna("")
    
    # Apply matching rules
    nLinks = dict()
    link_keys = pd.DataFrame(columns=['addressf', 'transactionid', 'lmk_key'])
    
    for rule in matching_rules:
        ppd_temp = ppd
        epc_temp = epc
        
        # Check filter requirements
        ppd_temp, epc_temp = filter_before_matching(ppd_temp, epc_temp, filter_requirements, rule)
        
        # Finalise matching keys
        ppd_temp['addressf'] = ppd_temp['postcode'].astype(str) + ", " + ppd_temp[matching_rules[rule][0]].astype(str)
        epc_temp['addressf'] = epc_temp['postcode'].astype(str) + ", " + epc_temp[matching_rules[rule][1]].astype(str)
        
        ppd_temp = ppd_temp[['addressf', 'transactionid']]
        epc_temp = epc_temp[['addressf', 'lmk_key']]
        
        # Inner join by matching keys
        new_link_keys = ppd_temp.join(epc_temp.set_index('addressf'), on='addressf', how='inner')
        
        # Collect new link keys
        link_keys = pd.concat([link_keys, new_link_keys], ignore_index=True)
        
        new_link_keys_count = new_link_keys.shape[0]
        new_unique_link_transactionids = new_link_keys['transactionid'].unique()
        new_unique_link_transactionid_count = len(new_unique_link_transactionids)
        prior_link_ppd_records_count = ppd_temp.shape[0]
        
        nLinks[rule] = [new_link_keys_count, new_unique_link_transactionid_count, prior_link_ppd_records_count]
        
        # Exclude matched records for next rounds
        ppd = ppd[~ppd['transactionid'].isin(list(new_unique_link_transactionids))]
        #epc = epc[~epc['lmk_key'].isin(list(ppd_epc_temp['lmk_key']))]
        # Append after-linked-records excluded count
        nLinks[rule].append(ppd.shape[0])
    
    link_keys.drop_duplicates(subset=['transactionid', 'lmk_key'], keep='first', inplace=True)
    
    return link_keys, nLinks, new_unique_link_transactionids, ppd, epc


# Define matching stages

def match_stage_1(ppd, epc):
    # ppd: transactionid, postcode, saon, paon, street, locality
    # epc: lmk_key, postcode, add, add1, add2
    ppd = ppd[['transactionid', 'postcode', 'saon', 'paon', 'street', 'locality']]
    epc = epc[['lmk_key', 'postcode', 'add', 'add1', 'add2']]
    
    # Define matching rules
    matching_rules = dict()
    matching_rules['rule 1'] = ['saon__paon_street', 'address_final1']
    matching_rules['rule 2'] = ['saon__paon__street', 'address_final1']
    matching_rules['rule 3'] = ['saon_paon1__street', 'address_final1']
    matching_rules['rule 4'] = ['saonn__paonn_streetn', 'address_final2']
    matching_rules['rule 5'] = ['saonn__paonn__streetn', 'address_final2']
    matching_rules['rule 6'] = ['saonn_paonn__streetn', 'address_final2']
    matching_rules['rule 7'] = ['saon_paon1__locality', 'address_final1']
    matching_rules['rule 8'] = ['saonn_paonn__localityn', 'address_final2']
    matching_rules['rule 9'] = ['saonn_paonn__localityn', 'address_final3']
    matching_rules['rule 10'] = ['saon__paon_street', 'address_final4']
    matching_rules['rule 11'] = ['saon__paon__street', 'address_final4']
    matching_rules['rule 12'] = ['saon_paon1__street', 'address_final4']
    matching_rules['rule 13'] = ['saonn__paonn_streetn', 'address_final5']
    matching_rules['rule 14'] = ['saonn__paonn__streetn', 'address_final5']
    matching_rules['rule 15'] = ['saonn_paonn__streetn', 'address_final5']
    matching_rules['rule 16'] = ['saon_paon1_street1', 'address_final4']
    matching_rules['rule 17'] = ['saonn_paonn_streetn1', 'address_final6']
    matching_rules['rule 18'] = ['saon__paon__street__locality', 'address_final1']
    matching_rules['rule 19'] = ['saonn__paonn__streetn__localityn', 'address_final2']
    matching_rules['rule 20'] = ['saon__paon__street__locality', 'address_final4']
    matching_rules['rule 21'] = ['saon_paon1_street1', 'address_final1']
    matching_rules['rule 22'] = ['saonn_paonn_streetn1', 'address_final2']
    matching_rules['rule 23'] = ['saon_paon1__locality', 'address_final4']
    matching_rules['rule 24'] = ['saonn_paonn__localityn', 'address_final5']
    matching_rules['rule 25'] = ['saon_paon2', 'address_final1']
    matching_rules['rule 26'] = ['saon_paon1_street2', 'address_final7']
    matching_rules['rule 27'] = ['saonn_paonn_streetn2', 'address_final8']
    
    # Define special filtering requirements
    filter_requirements = dict()
    
    
    # Create address variables from ppd
    # Rule 1, 10
    ppd['saon__paon'] = ppd[['saon','paon']].apply(", ".join, axis = 1)
    ppd['saon__paon_street'] = ppd['saon__paon'].astype(str) + " " + ppd['street'].astype(str)
    ppd['saon__paon_street'] = ppd['saon__paon_street'].str.strip().str.replace(" ", "", regex=False)
    # Rule 2, 11
    ppd['saon__paon__street'] = ppd['saon__paon'].astype(str) + ", " + ppd['street'].astype(str)
    ppd['saon__paon__street'] = ppd['saon__paon__street'].str.strip().str.replace(" ", "", regex=False)
    # Rule 3, 12
    ppd['saon_paon'] = ppd['saon'].astype(str) + " " + ppd['paon'].astype(str)
    ppd['saon_paon1'] = ppd['saon_paon'].str.strip()
    ppd['saon_paon1__street'] = ppd['saon_paon1'].astype(str) + ", " + ppd['street'].astype(str)
    ppd['saon_paon1__street'] = ppd['saon_paon1__street'].str.strip().str.replace(" ", "", regex=False)
    # Rule 4, 13
    ppd['saonn'] = ppd['saon'].str.replace("/", "", regex=False)
    ppd['paonn'] = ppd['paon'].str.replace("'", "", regex=False)
    ppd['paonn'] = ppd['paonn'].str.replace(".", "", regex=False)
    ppd['streetn'] = ppd['street'].str.replace("'", "", regex=False)
    ppd['saonn__paonn'] = ppd['saonn'].astype(str) + ", " + ppd['paonn'].astype(str)
    ppd['saonn__paonn_streetn'] = ppd['saonn__paonn'].astype(str) + " " + ppd['streetn'].astype(str)
    ppd['saonn__paonn_streetn'] = ppd['saonn__paonn_streetn'].str.strip().str.replace(" ", "", regex=False)
    # Rule 5, 14
    ppd['saonn__paonn__streetn'] = ppd['saonn__paonn'].astype(str) + ", " + ppd['streetn'].astype(str)
    ppd['saonn__paonn__streetn'] = ppd['saonn__paonn__streetn'].str.strip().str.replace(" ", "", regex=False)
    # Rule 6, 15
    ppd['saonn_paonn'] = ppd['saonn'].astype(str) + " " + ppd['paonn'].astype(str)
    ppd['saonn_paonn'] = ppd['saonn_paonn'].str.strip()
    ppd['saonn_paonn__streetn'] = ppd['saonn_paonn'].astype(str) + ", " + ppd['streetn'].astype(str)
    ppd['saonn_paonn__streetn'] = ppd['saonn_paonn__streetn'].str.strip().str.replace(" ", "", regex=False)
    # Rule 7, 23
    ppd['saon_paon1__locality'] = ppd['saon_paon1'].astype(str) + ", " + ppd['locality'].astype(str)
    ppd['saon_paon1__locality'] = ppd['saon_paon1__locality'].str.replace(" ", "", regex=False)
    # Rule 8-9, 24
    ppd['localityn'] = ppd['locality'].str.replace("'", "", regex=False)
    ppd['localityn'] = ppd['localityn'].str.replace(".", "", regex=False)
    ppd['saonn_paonn__localityn'] = ppd['saonn_paonn'].astype(str) + ", " + ppd['localityn'].astype(str)
    ppd['saonn_paonn__localityn'] = ppd['saonn_paonn__streetn'].str.replace(" ", "", regex=False)
    # Rule 16, 21
    ppd['saon_paon1_street1'] = ppd['saon_paon1'].astype(str) + " " + ppd['street'].astype(str)
    ppd['saon_paon1_street1'] = ppd['saon_paon1_street1'].str.strip().str.replace(" ", "", regex=False)
    # Rule 17, 22
    ppd['saonn_paonn_streetn1'] = ppd['saonn_paonn'].astype(str) + " " + ppd['streetn'].astype(str)
    ppd['saonn_paonn_streetn1'] = ppd['saonn_paonn_streetn1'].str.strip().str.replace(" ", "", regex=False)
    # Rule 18, 20
    ppd['saon__paon__street__locality'] = ppd['saon__paon__street'].astype(str) + ", " + ppd['locality'].astype(str)
    ppd['saon__paon__street__locality'] = ppd['saon__paon__street__locality'].str.replace(" ", "", regex=False)
    # Rule 19
    ppd['saonn__paonn__streetn__localityn'] = ppd['saonn__paonn__streetn'].astype(str) + ", " + ppd['localityn'].astype(str)
    ppd['saonn__paonn__streetn__localityn'] = ppd['saonn__paonn__streetn__localityn'].str.replace(" ", "", regex=False)
    # Rule 25
    ppd['saon_paon2'] = ppd['saon_paon'].str.replace(" ", "", regex=False)
    # Rule 26
    ppd['saon_paon1_street2'] = ppd['saon_paon1_street1'].str.strip().str.replace(",", "", regex=False)
    # Rule 27
    ppd['saonn_paonn_streetn2'] = ppd['saonn_paonn_streetn1'].str.strip().str.replace(",", "", regex=False)
    
    
    # Create address variables from epc
    # Rule 1-3, 7, 18
    epc['add_1'] = epc['add'].str.strip()
    epc['address_final1'] = epc['add_1'].str.replace(" ", "", regex=False)
    # Rule 4-6, 8, 19, 22
    epc['address_final2'] = epc['add_1'].str.replace("'", "", regex=False)
    epc['address_final2'] = epc['address_final2'].str.replace(".", "", regex=False)
    epc['address_final2'] = epc['address_final2'].str.replace("/", "", regex=False)
    epc['address_final2'] = epc['address_final2'].str.replace(" ", "", regex=False)
    # Rule 9
    epc['address_final3'] = epc['address_final2'].str.replace("-", "", regex=False)
    # Rule 10-12, 16, 23
    epc['add1__add2'] = epc['add1'].astype(str) + ", " + epc['add2'].astype(str)
    epc['add1__add2'] = epc['add1__add2'].str.replace(" ", "", regex=False)
    epc['address_final4'] = epc['add1__add2']
    # Rule 13-15, 24
    epc['address_final5'] = epc['address_final4'].str.replace("'", "", regex=False)
    epc['address_final5'] = epc['address_final5'].str.replace(".", "", regex=False)
    epc['address_final5'] = epc['address_final5'].str.replace("/", "", regex=False)
    # Rule 17
    epc['address_final6'] = epc['address_final5'].str.replace(",", "", regex=False)
    # Rule 26
    epc['address_final7'] = epc['address_final1'].str.replace(",", "", regex=False)
    # Rule 27
    epc['address_final8'] = epc['address_final2'].str.replace(",", "", regex=False)
    
    link_keys, nLinks, new_unique_link_transactionids, ppd, epc = match_keys(ppd, epc, matching_rules, filter_requirements)
    
    return link_keys, nLinks, new_unique_link_transactionids

def match_stage_2(ppd, epc):
    # NOTE: ppd records entering stage 2 are those with empty "saon"
    # ppd: transactionid, postcode, saon, paon, street, locality
    # epc: lmk_key, postcode, add, add1, add2
    ppd = ppd[['transactionid', 'postcode', 'saon', 'paon', 'street', 'locality']]
    epc = epc[['lmk_key', 'postcode', 'add', 'add1', 'add2']]  
    
    # Select ppd records with empty "saon"
    ppd = ppd[ppd['saon'] == ""]
    
    # Define matching rules
    matching_rules = dict()
    matching_rules['rule 28'] = ['paon__street__locality', 'address_final1']
    matching_rules['rule 29'] = ['paonn__streetn__localityn', 'address_final2']
    matching_rules['rule 30'] = ['paon__street__locality', 'address_final4']
    matching_rules['rule 31'] = ['paonn__streetn__localityn', 'address_final5']
    matching_rules['rule 32'] = ['paon_street__locality', 'address_final1']
    matching_rules['rule 33'] = ['paonn_streetn__localityn', 'address_final2']
    matching_rules['rule 34'] = ['paon_street__locality', 'address_final4']
    matching_rules['rule 35'] = ['paonn_streetn__localityn', 'address_final5']
    matching_rules['rule 36'] = ['paon_street_locality1', 'address_final9']
    matching_rules['rule 37'] = ['paon_street_locality1', 'address_final7']
    matching_rules['rule 38'] = ['paonn__streetn1', 'address_final10']
    matching_rules['rule 39'] = ['paonn2', 'address_final11']
    matching_rules['rule 40'] = ['paon_comma_sep1__paon_comma_sep2', 'address_final11']
    
    # Define special filtering requirements
    filter_requirements = dict()
    filter_requirements['rule 39'] = {'ppd': [['street', "equal", ""]]}    # for empty street record only
    filter_requirements['rule 40'] = {'ppd': [[',', "in", "paon"]]}    # for paon record having comma only
    
    
    # Create address variables from ppd
    # Rule 28, 30
    ppd['paon__street'] = ppd['paon'].astype(str) + ", " + ppd['street'].astype(str)
    ppd['paon__street__locality'] = ppd['paon__street'].astype(str) + ", " + ppd['locality'].astype(str)
    ppd['paon__street__locality'] = ppd['paon__street__locality'].str.replace(" ", "", regex=False)
    # Rule 29, 31
    ppd['paonn'] = ppd['paon'].str.replace("'", "", regex=False)
    ppd['paonn'] = ppd['paonn'].str.replace(".", "", regex=False)
    ppd['streetn'] = ppd['street'].str.replace("'", "", regex=False)
    ppd['localityn'] = ppd['locality'].str.replace("'", "", regex=False)
    ppd['localityn'] = ppd['localityn'].str.replace(".", "", regex=False)
    ppd['paonn__streetn'] = ppd['paonn'].astype(str) + ", " + ppd['streetn'].astype(str)
    ppd['paonn__streetn__localityn'] = ppd['paonn__streetn'].astype(str) + ", " + ppd['localityn'].astype(str)
    ppd['paonn__streetn__localityn'] = ppd['paonn__streetn__localityn'].str.replace(" ", "", regex=False)
    # Rule 32, 34
    ppd['paon_street'] = ppd['paon'].astype(str) + " " + ppd['street'].astype(str)
    ppd['paon_street__locality'] = ppd['paon_street'].astype(str) + ", " + ppd['locality'].astype(str)
    ppd['paon_street__locality'] = ppd['paon_street__locality'].str.replace(" ", "", regex=False)
    # Rule 33, 35
    ppd['paonn_streetn'] = ppd['paonn'].astype(str) + ", " + ppd['streetn'].astype(str)
    ppd['paonn_streetn__localityn'] = ppd['paonn_streetn'].astype(str) + ", " + ppd['localityn'].astype(str)
    ppd['paonn_streetn__localityn'] = ppd['paonn_streetn__localityn'].str.replace(" ", "", regex=False)
    # Rule 36, 37
    ppd['paon_street_locality'] = ppd['paon_street'].astype(str) + " " + ppd['locality'].astype(str)
    ppd['paon_street_locality'] = ppd['paon_street_locality'].str.replace(" ", "", regex=False)
    ppd['paon_street_locality1'] = ppd['paon_street_locality'].str.replace(",", "", regex=False)
    # Rule 38
    ppd['paonn__streetn1'] = ppd['paonn__streetn'].str.replace(" ", "", regex=False)
    # Rule 39
    ppd['paonn1'] = ppd['paonn'].str.replace(" ", "", regex=False)
    ppd['paonn2'] = ppd['paonn1'].str.replace("-", "", regex=False)
    # Rule 40
    ppd[['paon_comma_sep1', 'paon_comma_sep2']] = ppd['paon'].str.split(",", n=1, expand=True)
    ppd['paon_comma_sep1__paon_comma_sep2'] = ppd['paon_comma_sep1'].astype(str) + ", " + ppd['paon_comma_sep2'].astype(str)
    ppd['paon_comma_sep1__paon_comma_sep2'] = ppd['paon_comma_sep1__paon_comma_sep2'].str.replace(" ", "", regex=False)
    
    # Create address variables from epc
    # Rule 28, 32
    epc['add_1'] = epc['add'].str.strip()
    epc['address_final1'] = epc['add_1'].str.replace(" ", "", regex=False)
    # Rule 29, 33
    epc['address_final2'] = epc['add_1'].str.replace("'", "", regex=False)
    epc['address_final2'] = epc['address_final2'].str.replace(".", "", regex=False)
    epc['address_final2'] = epc['address_final2'].str.replace("/", "", regex=False)
    epc['address_final2'] = epc['address_final2'].str.replace(" ", "", regex=False)
    # Rule 
    epc['address_final3'] = epc['address_final2'].str.replace("-", "", regex=False)
    # Rule 30, 34
    epc['add1__add2'] = epc['add1'].astype(str) + ", " + epc['add2'].astype(str)
    epc['add1__add2'] = epc['add1__add2'].str.replace(" ", "", regex=False)
    epc['address_final4'] = epc['add1__add2']
    # Rule 31, 35
    epc['address_final5'] = epc['address_final4'].str.replace("'", "", regex=False)
    epc['address_final5'] = epc['address_final5'].str.replace(".", "", regex=False)
    epc['address_final5'] = epc['address_final5'].str.replace("/", "", regex=False)
    # Rule 
    epc['address_final6'] = epc['address_final5'].str.replace(",", "", regex=False)
    # Rule 37 
    epc['address_final7'] = epc['address_final1'].str.replace(",", "", regex=False)
    # Rule 
    epc['address_final8'] = epc['address_final2'].str.replace(",", "", regex=False)
    # Rule 36
    epc['address_final9'] = epc['address_final4'].str.replace(",", "", regex=False)
    # Rule 38
    epc['address_final10'] = epc['address_final5'].str.replace("-", "", regex=False)
    # Rule 39, 40
    epc['add1_1'] = epc['add1'].str.strip()
    epc['add1_1'] = epc['add1_1'].str.replace(" ", "", regex=False)
    epc['add1_2'] = epc['add1_1'].str.replace("'", "", regex=False)
    epc['add1_2'] = epc['add1_2'].str.replace(".", "", regex=False)
    epc['add1_2'] = epc['add1_2'].str.replace("/", "", regex=False)
    epc['add1_2'] = epc['add1_2'].str.replace("-", "", regex=False)
    epc['address_final11'] = epc['add1_2']
    
    
    link_keys, nLinks, new_unique_link_transactionids, ppd, epc = match_keys(ppd, epc, matching_rules, filter_requirements)
    
    return link_keys, nLinks, new_unique_link_transactionids

def match_stage_3_not_flat(ppd, epc):
    # NOTE: ppd records entering this stage are those with empty "saon" and non-F propertytype
    # ppd: transactionid, postcode, saon, paon, street, locality
    # epc: lmk_key, postcode, add, add1, add2
    ppd = ppd[['transactionid', 'postcode', 'saon', 'paon', 'street', 'locality', 'propertytype']]
    epc = epc[['lmk_key', 'postcode', 'add', 'add1', 'add2', 'add3', 'property_type']]
    
    # select the transaction which property type is not Flats/Maisonettes
    ppd = ppd[(ppd['saon'] == "") & (ppd['propertytype'] != "F")]
    
    # Define matching rules
    matching_rules = dict()
    matching_rules['rule 41'] = ['paon_finalword__street__locality', 'address_final1']
    matching_rules['rule 42'] = ['paon_finalword__street__locality', 'address_final4']
    matching_rules['rule 43'] = ['paonn_finalword__streetn__localityn', 'address_final3']
    matching_rules['rule 44'] = ['paon_finalword_street_locality1', 'address_final7']
    matching_rules['rule 45'] = ['paon_comma_sep1__street__locality', 'address_final14']
    matching_rules['rule 46'] = ['paon_comma_sep1_street_locality1', 'address_final13']
    matching_rules['rule 47'] = ['paon_comma_sep1_street_locality1', 'address_final15']
    matching_rules['rule 48'] = ['paon_comma_sep1_street_locality1', 'address_final6']
    matching_rules['rule 49'] = ['paon_comma_sep1__locality', 'address_final16']
    matching_rules['rule 50'] = ['paon_comma_sep1_street2', 'address_final17']
    matching_rules['rule 51'] = ['paon_comma_sep1_street2', 'address_final19']
    matching_rules['rule 52'] = ['paon_finalword_street2', 'address_final15']
    matching_rules['rule 53'] = ['paon_finalword_street2', 'add1_4']
    matching_rules['rule 54'] = ['paon_comma_sep1_paon_comma_sep2_street_locality2', 'address_final20']
    matching_rules['rule 55'] = ['paon_comma_sep1_paon_comma_sep2_street_locality2', 'address_final24']
    matching_rules['rule 56'] = ['paon_finalword_street_locality1', 'address_final27']
    matching_rules['rule 57'] = ['THE_paon_comma_sep1', 'add1']
    matching_rules['rule 58'] = ['paon__street__locality2', 'address_final29']
    matching_rules['rule 59'] = ['paon__street__locality2', 'address_final32']
    matching_rules['rule 60'] = ['paon__street__locality2', 'address_final33']
    matching_rules['rule 61'] = ['paon__street1', 'add1_5']
    matching_rules['rule 62'] = ['paon', 'add1']
    matching_rules['rule 63'] = ['paon__street__locality2', 'address_final35']
    matching_rules['rule 64'] = ['paon__street1', 'address_final37']
    matching_rules['rule 65'] = ['paon__street1', 'add1_firstword__add2']
    matching_rules['rule 66'] = ['paon__streetn_1_1', 'add1_5']
    matching_rules['rule 67'] = ['paon__street1', 'address_final38']
    

    # Define special filtering requirements
    filter_requirements = dict()
    filter_requirements['rule 41'] = {'ppd': [[",", "innotregex", "paon"]]}    # for paon record having comma only
    filter_requirements['rule 42'] = {'ppd': [[",", "innotregex", "paon"]]}    # for paon record having comma only
    filter_requirements['rule 43'] = {'ppd': [[",", "innotregex", "paon"]]}    # for paon record having comma only
    filter_requirements['rule 44'] = {'ppd': [[",", "innotregex", "paon"]]}    # for paon record having comma only
    filter_requirements['rule 45'] = {'ppd': [[",", "innotregex", "paon"]]}    # for paon record having comma only
    filter_requirements['rule 46'] = {'ppd': [[",", "innotregex", "paon"]]}    # for paon record having comma only
    filter_requirements['rule 47'] = {'ppd': [[",", "innotregex", "paon"]]}    # for paon record having comma only
    filter_requirements['rule 48'] = {'ppd': [[",", "innotregex", "paon"]]}    # for paon record having comma only
    filter_requirements['rule 49'] = {'ppd': [[",", "innotregex", "paon"]]}    # for paon record having comma only
    filter_requirements['rule 50'] = {'ppd': [[",", "innotregex", "paon"]]}    # for paon record having comma only
    filter_requirements['rule 51'] = {'ppd': [[",", "innotregex", "paon"]]}    # for paon record having comma only
    filter_requirements['rule 52'] = {'ppd': [[",", "innotregex", "paon"]]}    # for paon record having comma only
    filter_requirements['rule 53'] = {'ppd': [[",", "innotregex", "paon"]]}    # for paon record having comma only
    filter_requirements['rule 54'] = {'ppd': [[",", "innotregex", "paon"]]}    # for paon record having comma only
    filter_requirements['rule 55'] = {'ppd': [[",", "innotregex", "paon"]]}    # for paon record having comma only
    filter_requirements['rule 56'] = {'ppd': [[",", "innotregex", "paon"]],
                                      'epc': [["property_type", "notequal", "Flat"],
                                              ["property_type", "notequal", "Maisonette"]]}    
    filter_requirements['rule 57'] = {'ppd': [[",", "innotregex", "paon"]],
                                      'epc': [["property_type", "notequal", "Flat"],
                                              ["property_type", "notequal", "Maisonette"]]}
    filter_requirements['rule 63'] = {'epc': [[",", "innotregex", "add1"],
                                              ["-", "innotregex", "add1_comma_sep2_1"]]}
    filter_requirements['rule 64'] = {'epc': [[",", "innotregex", "add1"],
                                              [".", "innotregex", "add1_comma_sep2_1"]]}  
    filter_requirements['rule 65'] = {'ppd': [["street", "notequal", ""]],
                                      'epc': [["\s\D(\s|,|\>)", "notinregex", "add1"],
                                              ["^\d", "notinregex", "add2"],
                                              ["property_type", "notequal", "Flat"],
                                              ["property_type", "notequal", "Maisonette"]]}
    filter_requirements['rule 67'] = {'epc': [["property_type", "notequal", "Flat"],
                                              ["property_type", "notequal", "Maisonette"]]}
    
    
    
    # Create address variables from ppd
    # Rule 41, 42
    ppd['paon_finalword'] = ppd['paon'].str.split(" ", n=-1, expand=False).str[-1]
    ppd['paon_finalword__street'] = ppd['paon_finalword'].astype(str) + ", " + ppd['street'].astype(str)
    ppd['paon_finalword__street__locality'] = ppd['paon_finalword__street'].astype(str) + ", " + ppd['locality'].astype(str)
    ppd['paon_finalword__street__locality'] = ppd['paon_finalword__street__locality'].str.replace(" ", "", regex=False)
    # Rule 43
    ppd['saonn'] = ppd['saon'].str.replace("/", "", regex=False)
    ppd['paonn'] = ppd['paon'].str.replace("'", "", regex=False)
    ppd['paonn'] = ppd['paonn'].str.replace(".", "", regex=False)
    ppd['streetn'] = ppd['street'].str.replace("'", "", regex=False)
    ppd['localityn'] = ppd['locality'].str.replace("'", "", regex=False)
    ppd['localityn'] = ppd['localityn'].str.replace(".", "", regex=False)
    ppd['paonn_finalword'] = ppd['paonn'].str.split(" ", n=-1, expand=False).str[-1]
    ppd['paonn_finalword__streetn'] = ppd['paonn_finalword'].astype(str) + ", " + ppd['streetn'].astype(str)
    ppd['paonn_finalword__streetn__localityn'] = ppd['paonn_finalword__streetn'].astype(str) + ", " + ppd['localityn'].astype(str)
    ppd['paonn_finalword__streetn__localityn'] = ppd['paonn_finalword__streetn__localityn'].str.replace(" ", "", regex=False)
    # Rule 44, 56
    ppd['paon_finalword_street'] = ppd['paon_finalword'].astype(str) + " " + ppd['street'].astype(str)
    ppd['paon_finalword_street_locality'] = ppd['paon_finalword_street'].astype(str) + " " + ppd['locality'].astype(str)
    ppd['paon_finalword_street_locality'] = ppd['paon_finalword_street_locality'].str.replace(" ", "", regex=False)
    ppd['paon_finalword_street_locality1'] = ppd['paon_finalword_street_locality'].str.replace(",", "", regex=False)
    # Rule 45
    ppd[['paon_comma_sep1', 'paon_comma_sep2']] = ppd['paon'].str.split(",", n=1, expand=True)
    ppd['paon_comma_sep1__street'] = ppd['paon_comma_sep1'].astype(str) + ", " + ppd['street'].astype(str)
    ppd['paon_comma_sep1__street__locality'] = ppd['paon_comma_sep1__street'].astype(str) + ", " + ppd['locality'].astype(str)
    ppd['paon_comma_sep1__street__locality'] = ppd['paon_comma_sep1__street__locality'].str.replace(" ", "", regex=False)
    # Rule 46-48
    ppd['paon_comma_sep1_street'] = ppd['paon_comma_sep1'].astype(str) + " " + ppd['street'].astype(str)
    ppd['paon_comma_sep1_street_locality'] = ppd['paon_comma_sep1_street'].astype(str) + " " + ppd['locality'].astype(str)
    ppd['paon_comma_sep1_street_locality'] = ppd['paon_comma_sep1_street_locality'].str.replace(" ", "", regex=False)
    ppd['paon_comma_sep1_street_locality1'] = ppd['paon_comma_sep1_street_locality'].str.replace(",", "", regex=False)
    # Rule 49
    ppd['paon_comma_sep1__locality'] = ppd['paon_comma_sep1'].astype(str) + ", " + ppd['locality'].astype(str)
    ppd['paon_comma_sep1__locality'] = ppd['paon_comma_sep1__locality'].str.replace(" ", "", regex=False)
    # Rule 50, 51
    ppd['paon_comma_sep1_street1'] = ppd['paon_comma_sep1_street'].str.replace(" ", "", regex=False)
    ppd['paon_comma_sep1_street2'] = ppd['paon_comma_sep1_street1'].str.replace(",", "", regex=False)
    # Rule 52, 53
    ppd['paon_finalword_street1'] = ppd['paon_finalword_street'].str.replace(" ", "", regex=False)
    ppd['paon_finalword_street2'] = ppd['paon_finalword_street1'].str.replace(",", "", regex=False)
    # Rule 54, 55
    ppd['paon_comma_sep1_paon_comma_sep2'] = ppd['paon_comma_sep1'].astype(str) + " " + ppd['paon_comma_sep2'].astype(str)
    ppd['paon_comma_sep1_paon_comma_sep2_street'] = ppd['paon_comma_sep1_paon_comma_sep2'].astype(str) + " " + ppd['street'].astype(str)
    ppd['paon_comma_sep1_paon_comma_sep2_street_locality'] = ppd['paon_comma_sep1_paon_comma_sep2_street'].astype(str) + " " + ppd['locality'].astype(str)
    ppd['paon_comma_sep1_paon_comma_sep2_street_locality1'] = ppd['paon_comma_sep1_paon_comma_sep2_street_locality'].str.replace(" ", "", regex=False)
    ppd['paon_comma_sep1_paon_comma_sep2_street_locality2'] = ppd['paon_comma_sep1_paon_comma_sep2_street_locality1'].str.replace(",", "", regex=False)
    # Rule 57
    ppd['THE_paon_comma_sep1'] = "THE" + " " + ppd['paon_comma_sep1'].astype(str)
    # Rule 58-60, 63
    ppd['paon__street'] = ppd['paon'].astype(str) + ", " + ppd['street'].astype(str)
    ppd['paon__street__locality'] = ppd['paon__street'].astype(str) + ", " + ppd['locality'].astype(str)
    ppd['paon__street__locality1'] = ppd['paon__street__locality'].str.replace(" ", "", regex=False)
    ppd['paon__street__locality2'] = ppd['paon__street__locality1'].str.replace(",", "", regex=False)
    # Rule 61, 64, 65, 67
    ppd['paon__street1'] = ppd['paon__street'].str.replace(" ", "", regex=False)
    ppd['paon__street1'] = ppd['paon__street1'].str.replace(",", "", regex=False)
    # Rule 66, 
    ppd['streetn_1'] = ppd['street'].str.replace(".", "", regex=False)
    ppd['streetn_1'] = ppd['streetn_1'].str.replace("'", "", regex=False)
    ppd['streetn_1'] = ppd['streetn_1'].str.replace("-", "", regex=False)
    ppd['paon__streetn_1'] = ppd['paon'].astype(str) + ", " + ppd['street'].astype(str)
    ppd['paon__streetn_1'] = ppd['paon__streetn_1'].str.replace(" ", "", regex=False)
    ppd['paon__streetn_1_1'] = ppd['paon__streetn_1'].str.replace(",", "", regex=False)
    
    
    # Create address variables from epc
    
    
    # Rule 41
    epc['address_final1'] = epc['add'].str.replace(" ", "", regex=False)
    # Rule 
    epc['address_final2'] = epc['address_final1'].str.replace("'", "", regex=False)
    epc['address_final2'] = epc['address_final2'].str.replace(".", "", regex=False)
    epc['address_final2'] = epc['address_final2'].str.replace("/", "", regex=False)
    # Rule 43
    epc['address_final3'] = epc['address_final2'].str.replace("-", "", regex=False)
    # Rule 42
    epc['add1__add2'] = epc['add1'].astype(str) + ", " + epc['add2'].astype(str)
    epc['add1__add2_1'] = epc['add1__add2'].str.replace(" ", "", regex=False)
    epc['address_final4'] = epc['add1__add2_1']
    # Rule 
    epc['address_final5'] = epc['address_final4'].str.replace("'", "", regex=False)
    epc['address_final5'] = epc['address_final5'].str.replace(".", "", regex=False)
    epc['address_final5'] = epc['address_final5'].str.replace("/", "", regex=False)
    # Rule 48
    epc['address_final6'] = epc['address_final5'].str.replace(",", "", regex=False)
    # Rule 
    epc['address_final7'] = epc['address_final1'].str.replace(",", "", regex=False)
    # Rule 
    epc['address_final8'] = epc['address_final2'].str.replace(",", "", regex=False)
    # Rule 
    epc['address_final9'] = epc['address_final4'].str.replace(",", "", regex=False)
    # Rule 
    epc['address_final10'] = epc['address_final5'].str.replace("-", "", regex=False)
    # Rule 
    epc['add1_1'] = epc['add1'].str.strip()
    epc['add1_1'] = epc['add1_1'].str.replace(" ", "", regex=False)
    epc['add1_2'] = epc['add1_1'].str.replace("'", "", regex=False)
    epc['add1_2'] = epc['add1_2'].str.replace(".", "", regex=False)
    epc['add1_2'] = epc['add1_2'].str.replace("/", "", regex=False)
    epc['add1_3'] = epc['add1_2'].str.replace("-", "", regex=False)
    epc['add1_4'] = epc['add1_2'].str.replace(",", "", regex=False)
    # Rule 66
    epc['add1_5'] = epc['add1_1'].str.replace(",", "", regex=False)
    epc['address_final11'] = epc['add1_2']
    # Rule
    epc['add_1'] = epc['add'].str.strip()
    epc['address_final12'] = epc['add_1'].str.replace(" ", "", regex=False)
    # Rule 46
    epc['address_final13'] = epc['address_final12'].str.replace(",", "", regex=False)
    # Rule 45
    epc['address_final14'] = epc['address_final12'].str.replace("'", "", regex=False)
    epc['address_final14'] = epc['address_final14'].str.replace(".", "", regex=False)
    epc['address_final14'] = epc['address_final14'].str.replace("/", "", regex=False)
    # Rule 47
    epc['address_final15'] = epc['address_final14'].str.replace(",", "", regex=False)
    # Rule 49
    epc['add1__add2_2'] = epc['add1__add2'].str.strip()
    epc['add1__add2_2'] = epc['add1__add2_2'].str.replace(" ", "", regex=False)
    epc['address_final16'] = epc['add1__add2_2'].str.replace("'", "", regex=False)
    epc['address_final16'] = epc['address_final16'].str.replace(".", "", regex=False)
    epc['address_final16'] = epc['address_final16'].str.replace("/", "", regex=False)
    # Rule 50
    epc['address_final17'] = epc['address_final16'].str.replace(",", "", regex=False)
    # Rule 
    epc['add1__add3'] = epc['add1'].astype(str) + ", " + epc['add3'].astype(str)
    epc['add1__add3_1'] = epc['add1__add3'].str.replace(" ", "", regex=False)
    epc['add1__add3_2'] = epc['add1__add3'].str.strip()
    epc['add1__add3_2'] = epc['add1__add3_2'].str.replace(" ", "", regex=False)
    # Rule
    epc['address_final18'] = epc['add1__add3_2'].str.replace("'", "", regex=False)
    epc['address_final18'] = epc['address_final18'].str.replace(".", "", regex=False)
    epc['address_final18'] = epc['address_final18'].str.replace("/", "", regex=False)
    # Rule 51
    epc['address_final19'] = epc['address_final18'].str.replace(",", "", regex=False)
    # Rule 54
    epc['address_final20'] = epc['address_final3'].str.replace(",", "", regex=False)
    # Rule 
    epc['add1_add2'] = epc['add1'].astype(str) + " " + epc['add2'].astype(str)
    epc['add1_add2_1'] = epc['add1_add2'].str.replace(" ", "", regex=False)
    epc['add1_add2_2'] = epc['add1_add2'].str.strip()
    epc['add1_add2_2'] = epc['add1_add2_2'].str.replace(" ", "", regex=False)
    # Rule 
    epc['address_final21'] = epc['add1_add2_2'].str.replace(",", "", regex=False)
    # Rule
    epc['address_final22'] = epc['add1_add2_2'].str.replace("'", "", regex=False)
    epc['address_final22'] = epc['address_final22'].str.replace(".", "", regex=False)
    epc['address_final22'] = epc['address_final22'].str.replace("/", "", regex=False)
    # Rule 55
    epc['address_final23'] = epc['address_final22'].str.replace(",", "", regex=False)
    # Rule
    epc['address_final24'] = epc['address_final23'].str.replace("-", "", regex=False)
    # Rule 
    epc['add2_add3'] = epc['add2'].astype(str) + " " + epc['add3'].astype(str)
    epc['add2_add3_1'] = epc['add2_add3'].str.replace(" ", "", regex=False)
    epc['add2_add3_2'] = epc['add2_add3'].str.strip()
    epc['add2_add3_2'] = epc['add2_add3_2'].str.replace(" ", "", regex=False)
    # Rule 
    epc['address_final25'] = epc['add2_add3_2'].str.replace(",", "", regex=False)
    # Rule
    epc['address_final26'] = epc['add2_add3_2'].str.replace("'", "", regex=False)
    epc['address_final26'] = epc['address_final26'].str.replace(".", "", regex=False)
    epc['address_final26'] = epc['address_final26'].str.replace("/", "", regex=False)
    # Rule 56
    epc['address_final27'] = epc['address_final26'].str.replace(",", "", regex=False)
    # Rule 
    epc['address_final28'] = epc['address_final27'].str.replace("-", "", regex=False)
    # Rule 58
    epc['add2_1'] = epc['add2'].str.replace("-", "", regex=False)
    epc['add1_add2_1'] = epc['add1'].astype(str) + " " + epc['add2_1'].astype(str)
    epc['address_final29'] = epc['add1_add2_1'].str.replace(" ", "", regex=False)
    epc['address_final29'] = epc['address_final29'].str.replace("'", "", regex=False)
    epc['address_final29'] = epc['address_final29'].str.replace(".", "", regex=False)
    epc['address_final29'] = epc['address_final29'].str.replace("/", "", regex=False)
    epc['address_final29'] = epc['address_final29'].str.replace(",", "", regex=False)
    # Rule 
    epc['add1_add3'] = epc['add1'].astype(str) + " " + epc['add3'].astype(str)
    epc['add1_add3_1'] = epc['add1_add3'].str.replace(" ", "", regex=False)
    epc['add1_add3_2'] = epc['add1_add3'].str.strip()
    epc['add1_add3_2'] = epc['add1_add3_2'].str.replace(" ", "", regex=False)
    # Rule
    epc['address_final30'] = epc['add1_add3_2'].str.replace("'", "", regex=False)
    epc['address_final30'] = epc['address_final30'].str.replace(".", "", regex=False)
    epc['address_final30'] = epc['address_final30'].str.replace("/", "", regex=False)
    # Rule 
    epc['address_final31'] = epc['add1_add3_2'].str.replace(",", "", regex=False)
    # Rule 59
    epc['address_final32'] = epc['address_final30'].str.replace(",", "", regex=False)
    # Rule 60
    epc['address_final33'] = epc['add1_add3_1'].str.replace(",", "", regex=False)
    # Rule 63
    epc[['add1_comma_sep1', 'add1_comma_sep2']] = epc['add1'].str.split(",", n=1, expand=True)
    epc['add1_comma_sep2_1'] = epc['add1_comma_sep2'].str.replace("-", "", regex=False)
    epc['add1_comma_sep1__add1_comma_sep2_1'] = epc['add1_comma_sep1'].astype(str) + ", " + epc['add1_comma_sep2_1'].astype(str)
    epc['add1_comma_sep1__add1_comma_sep2_1__add2'] = epc['add1_comma_sep1__add1_comma_sep2_1'].astype(str) + ", " + epc['add2'].astype(str)
    epc['address_final34'] = epc['add1_comma_sep1__add1_comma_sep2_1__add2'].str.replace(" ", "", regex=False)
    epc['address_final35'] = epc['address_final34'].str.replace(",", "", regex=False)
    # Rule 64
    epc['add1_comma_sep2_2'] = epc['add1_comma_sep2'].str.replace(".", "", regex=False)
    epc['add1_comma_sep1__add1_comma_sep2_2'] = epc['add1_comma_sep1'].astype(str) + ", " + epc['add1_comma_sep2_2'].astype(str)
    epc['address_final36'] = epc['add1_comma_sep1__add1_comma_sep2_2'].str.replace(" ", "", regex=False)
    epc['address_final37'] = epc['address_final36'].str.replace(",", "", regex=False)
    # Rule 65
    epc['add1_firstword'] = epc['add1'].str.split(" ", n=1, expand=False).str[0]
    epc['add1_firstword__add2'] = epc['add1_firstword'].astype(str) + ", " + epc['add2'].astype(str)
    # Rule 67
    epc['address_final38'] = epc['address_final13'].str.replace("UNIT ", "", regex=False)
    
    link_keys, nLinks, new_unique_link_transactionids, ppd, epc = match_keys(ppd, epc, matching_rules, filter_requirements)
    
    return link_keys, nLinks, new_unique_link_transactionids

def match_stage_3_flat(ppd, epc):
    # NOTE
    # ppd: transactionid, postcode, saon, paon, street, locality
    # epc: lmk_key, postcode, add, add1, add2
    # select the transaction which property type is Flats/Maisonettes
    ppd = ppd[ppd['propertytype'] == "F"]

    
    

def match_stage_4(ppd, epc):
    # ppd records entering stage 2 are those with empty "saon"
    # ppd: 
    # epc:
    pass