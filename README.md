# DSAI3202-Labs

This is my attempt at doing the fabric 
let
Source = AzureStorage.DataLake(
    "https://goodreadsreviews60302363.dfs.core.windows.net/lakehouse/gold/curated_reviews/",
    [HierarchicalNavigation=true]
  ),
  DeltaTable = DeltaLake.Table(Source),
  #"Changed column type" = Table.TransformColumnTypes(DeltaTable, {{"n_votes", Int64.Type}}),
  #"Removed blank rows" = Table.SelectRows(#"Changed column type", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
  #"Removed blank rows 1" = Table.SelectRows(#"Removed blank rows", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
  #"Removed blank rows 2" = Table.SelectRows(#"Removed blank rows 1", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
  #"Removed blank rows 3" = Table.SelectRows(#"Removed blank rows 2", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
  #"Removed blank rows 4" = Table.SelectRows(#"Removed blank rows 3", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
  #"Added custom" = Table.AddColumn(#"Removed blank rows 4", "review_length", each Text.Length([review_text])),
  #"Filtered rows" = Table.SelectRows(#"Added custom", each [review_length] >= 10),
  #"Replaced value" = Table.ReplaceValue(#"Filtered rows", null, 0, Replacer.ReplaceValue, {"n_votes"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "", "Unknown", Replacer.ReplaceValue, {"language"}),
  #"Trimmed text" = Table.TransformColumns(#"Replaced value 1", {{"title", each Text.Trim(_), type nullable text}, {"name", each Text.Trim(_), type nullable text}, {"review_text", each Text.Trim(_), type nullable text}}),
  #"Capitalized each word" = Table.TransformColumns(#"Trimmed text", {{"title", each Text.Proper(_), type nullable text}, {"name", each Text.Proper(_), type nullable text}}),
  #"Lowercased text" = Table.TransformColumns(#"Capitalized each word", {{"language", each Text.Lower(_), type nullable text}}),
  #"Added custom 1" = Table.AddColumn(#"Lowercased text", "word_count", each List.Count(Text.Split(Text.Trim([review_text]), " "))),
  #"Grouped rows" = Table.Group(#"Added custom 1", {"book_id"}, {{"avg_word_count_book", each List.Average([word_count]), type any}})
in
#"Grouped rows"
