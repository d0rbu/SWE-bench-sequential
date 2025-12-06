from chain_construction import construct_chains_from_prs

if __name__ == '__main__':
    prs_file = 'multi_turn_test/sklearn-prs.jsonl'
    output_file = 'multi_turn_test/sklearn-chains.jsonl'
    # Specify how many of the best chains we want to keep
    construct_chains_from_prs(prs_file, output_file, target_chains=5)