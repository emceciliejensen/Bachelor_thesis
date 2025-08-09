import pandas as pd

def compute_self_citation_stats(full_path, self_path):
    # Load datasets
    df_all = pd.read_csv(full_path)
    df_self = pd.read_csv(self_path)

    # Total counts
    total_citations = len(df_all)
    total_self = len(df_self)

    # Gender-specific self-citation counts
    male_self = len(df_self[df_self['SourceGender'] == 'male'])
    female_self = len(df_self[df_self['SourceGender'] == 'female'])

    # Percent of total citations
    percent_self_total = (total_self / total_citations) * 100 if total_citations else 0
    percent_male_total = (male_self / total_citations) * 100 if total_citations else 0
    percent_female_total = (female_self / total_citations) * 100 if total_citations else 0

    # Percent of self-citations
    percent_male_self = (male_self / total_self) * 100 if total_self else 0
    percent_female_self = (female_self / total_self) * 100 if total_self else 0

    # Output results
    print("Physics Self-Citation Statistics")
    print(f"Total citations:              {total_citations:,}")
    print(f"Total self-citations:         {total_self:,} ({percent_self_total:.2f}%)\n")

    print(f"Male self-citations:          {male_self:,}")
    print(f"Female self-citations:        {female_self:,}\n")

    print(f"Male self-citations as % of total:   {percent_male_total:.2f}%")
    print(f"Female self-citations as % of total: {percent_female_total:.2f}%\n")

    print(f"Male self-citations as % of self:    {percent_male_self:.2f}%")
    print(f"Female self-citations as % of self:  {percent_female_self:.2f}%")

# === Example run ===
if __name__ == "__main__":
    full_path = "Physics_Author_Citations_random_sample_Genderized.csv"
    self_path = "Physics_random_sample_self_citations.csv"
    compute_self_citation_stats(full_path, self_path)






#import pandas as pd

#def compute_and_save_self_citation_summary(full_path, self_path, output_csv):
#    df_full = pd.read_csv(full_path)
#    df_self = pd.read_csv(self_path)

#    total = len(df_full)
#    self_cites = len(df_self)
#    percent = round((self_cites / total) * 100, 2) if total else 0.0

#    result = pd.DataFrame([{
#        "TotalCitations": total,
#        "SelfCitations": self_cites,
#        "PercentSelfCitations": percent
#    }])

#    result.to_csv(output_csv, index=False)
#    print(f"Stats saved to {output_csv}")


#compute_and_save_self_citation_summary(
#     "/home/emcj/data/MAG/Psychology_Author_Citations_Genderized.csv",
#     "/home/emcj/data/MAG/Psychology_self_citations.csv",
#     "/home/emcj/data/MAG/Psychology_self_summary.csv"
#)
