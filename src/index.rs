use tantivy::schema::{Schema, STORED, STRING, TEXT};

pub fn schema() -> anyhow::Result<Schema> {
    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("iri", STRING | STORED);
    schema_builder.add_text_field("text", TEXT);
    Ok(schema_builder.build())
}
