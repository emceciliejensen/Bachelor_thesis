import pandas as pd

def check_self_citations_with_gender(csv_path, output_path='/home/emcj/data/MAG/Physics_random_sample_self_citations.csv'):
    df = pd.read_csv(csv_path)

    self_cites = df[df['SourceAuthorId'] == df['TargetAuthorId']]

    if self_cites.empty:
        print("No self-citations found.")
    else:
        print(f"Found {len(self_cites)} self-citation(s). Saving to {output_path}.")

        print("\nSample of self-citations with gender info:")
        print(self_cites[['SourceAuthorId', 'SourceGender', 'TargetGender']].head())

        self_cites.to_csv(output_path, index=False)

        # Optional: Gender summary
        print("\nSelf-citation count by SourceGender:")
        print(self_cites['SourceGender'].value_counts(dropna=False))

        if 'TargetGender' in self_cites.columns:
            print("\nCross-tabulation of SourceGender and TargetGender:")
            print(pd.crosstab(self_cites['SourceGender'], self_cites['TargetGender'], dropna=False))

    return self_cites


check_self_citations_with_gender("/home/emcj/data/MAG/Physics_Author_Citations_random_sample_Genderized.csv")
