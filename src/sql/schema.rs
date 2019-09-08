use super::types::DataType;

/// A table
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: String,
}

impl Table {
    pub fn get_primary_key_index(&self) -> usize {
        self.columns.iter().position(|c| c.name == self.primary_key).unwrap()
    }

    pub fn to_query(&self) -> String {
        let mut query = format!("CREATE TABLE {} (\n", self.name);
        for column in self.columns.iter() {
            query += &format!(
                "  {} {}",
                column.name,
                match column.datatype {
                    DataType::Boolean => "BOOLEAN",
                    DataType::Float => "FLOAT",
                    DataType::Integer => "INTEGER",
                    DataType::String => "VARCHAR",
                }
            );
            if self.primary_key == column.name {
                query += " PRIMARY KEY";
            }
            query += if column.nullable { " NULL" } else { " NOT NULL" };
            query += ",\n";
        }
        query += ")";
        query
    }
}

/// A table column
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
}
