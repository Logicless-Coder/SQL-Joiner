use serde::Deserialize;
use std::{cmp, env, fs, io, path::Path};

#[derive(Deserialize, Debug)]
struct Column {
    name: String,
    #[serde(default)]
    indexed: bool,
    total_values: u32
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Table {
    name: String,
    columns: Vec<Column>,
    sorted_column: Column,
    nr: u32,
    br: u32,
}

#[derive(Debug)]
enum JoinerError {
    IO(io::Error),
    Parse(serde_json::Error),
}

impl From<io::Error> for JoinerError {
    fn from(err: io::Error) -> Self {
        JoinerError::IO(err)
    }
}

impl From<serde_json::Error> for JoinerError {
    fn from(err: serde_json::Error) -> Self {
        JoinerError::Parse(err)
    }
}

fn load_json_from_file<P: AsRef<Path>>(path: P) -> Result<Vec<Table>, JoinerError> {
    let content = fs::read_to_string(path)?;
    let tables: Vec<Table> = serde_json::from_str(&content)?;

    Ok(tables)
}

/*
 * The input format: <table1>.<column1> = <table2>.<column2>
 * For example,
 * Orders.cust_id = Customers.id
 */
fn read_user_input() -> Result<((String, String), (String, String)), JoinerError> {
    let mut buffer = String::new();
    io::stdin().read_line(&mut buffer)?;

    let halves: Vec<&str> = buffer.split("=").to_owned().collect();
    let left: Vec<&str> = halves[0].trim().split(".").collect();
    let right: Vec<&str> = halves[1].trim().split(".").collect();

    let table1 = match left.get(0) {
        Some(x) => x.to_string(),
        None => panic!("Input format: <table1>.<column1> = <table2>.<column2>")
    };
    let column1 = match left.get(1) {
        Some(x) => x.to_string(),
        None => panic!("Input format: <table1>.<column1> = <table2>.<column2>")
    };
    let table2 = match right.get(0) {
        Some(x) => x.to_string(),
        None => panic!("Input format: <table1>.<column1> = <table2>.<column2>")
    };
    let column2 = match right.get(1) {
        Some(x) => x.to_string(),
        None => panic!("Input format: <table1>.<column1> = <table2>.<column2>")
    };

    Ok(((table1, column1), (table2, column2)))
}

fn height_of_index_tree(n: u32, k: u32) -> u32 {
    ((k as f32).log2() / ((n/2) as f32).log2()).ceil() as u32
}

fn block_nested_join_cost(table1: &Table, table2: &Table, memory_size: u32) -> u32 {
    let smaller: u32 = cmp::min(table1.br, table2.br);
    if smaller < memory_size {
        table1.br + table2.br
    } else {
        smaller * (table1.br + table2.br - smaller + 1)
    }
} 

fn indexed_join_cost(table1: &Table, column1: &Column, table2: &Table, column2: &Column) -> Option<u32> {
    let n = 10;
    let mut cost: Option<u32> = None;
    if column1.indexed {
        let lookup_cost1: u32 = height_of_index_tree(n, column1.total_values);
        let total_cost1: u32 = table2.nr * lookup_cost1 + table2.br;
        cost = match cost {
            None => Some(total_cost1),
            Some(x) => Some(cmp::min(x, total_cost1))
        }
    }
    if column2.indexed {
        let lookup_cost2: u32 = height_of_index_tree(n, column2.total_values);
        let total_cost2: u32 = table1.nr * lookup_cost2 + table1.br;
        cost = match cost {
            None => Some(total_cost2),
            Some(x) => Some(cmp::min(x, total_cost2))
        }
    }
    
    cost
}

fn sorting_cost(br: u32, memory_size: u32) -> u32 {
    let tmp = ((br / memory_size) as f32).ceil();
    2 * br * (tmp as f32).log((memory_size - 1) as f32).ceil() as u32
}

fn merge_join_cost(table1: &Table, column1: &Column, table2: &Table, column2: &Column, memory_size: u32) -> u32 {
    let mut cost_to_sort: u32 = 0;
    if table1.sorted_column.name != column1.name {
        cost_to_sort += sorting_cost(table1.br, memory_size);
    }
    if table2.sorted_column.name != column2.name {
        cost_to_sort += sorting_cost(table2.br, memory_size);
    }
    
    cost_to_sort + table1.br + table2.br
}

fn hash_join_cost(table1: &Table, table2: &Table, memory_size: u32) -> Option<u32> {
    let smaller: &Table = cmp::min_by_key(table1, table2, |x: &&Table| x.br); 
    if memory_size * memory_size > smaller.br {
        let nh: u32 = ((smaller.br / memory_size) as f32).ceil() as u32 + 1;
        return Some(3 * (table1.br + table2.br) + nh)
    } 
    None
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let binary = &args[0];
    let path = match args.get(1) {
        Some(x) => x,
        None => panic!("Usage: {binary} <path to database metadata> <memory size=10,000>?"),
    };
    let memory_size: u32 = match args.get(2) {
        Some(x) => match x.parse() {
            Ok(xx) => xx,
            Err(_) => panic!("Memory size should be a whole number")
        },
        None => 10_000,
    };

    let data = match load_json_from_file(path) {
        Ok(x) => x,
        Err(JoinerError::IO(err)) => panic!("IO error {err}"),
        Err(JoinerError::Parse(err)) => panic!("Parse error {err}"),
    };

    println!("TABLES =>");
    for table in &data {
        println!("{}", table.name);
        for column in &table.columns {
            println!(" - {}", column.name);
        }
        println!();
    }

    let ((table1_name, column1_name), (table2_name, column2_name)) = match read_user_input() {
        Ok(x) => x,
        Err(JoinerError::IO(err)) => panic!("Error reading user input {err}"),
        Err(JoinerError::Parse(err)) => panic!("Error reading user input {err}"),
    };

    let (mut table1, mut table2): (Option<&Table>, Option<&Table>) = (None, None);
    let (column1, column2): (Option<&Column>, Option<&Column>);
    for table in &data {
        if table.name == table1_name {
            table1 = Some(table);
        } else if table.name == table2_name {
            table2 = Some(table);
        }        
    }

    column1 = match table1 {
        None => panic!("Table not found with name {table1_name}"),
        Some(t) => {
            let mut ret_val: Option<&Column> = None;
            for column in &t.columns {
                if column.name == column1_name {
                    ret_val = Some(column);
                }
            }
            ret_val
        }
    };
    column2 = match table2 {
        None => panic!("Table not found with name {table2_name}"),
        Some(t) => {
            let mut ret_val: Option<&Column> = None;
            for column in &t.columns {
                if column.name == column2_name {
                    ret_val = Some(column);
                }
            }
            ret_val
        }
    };

    if column1.is_none() {
        panic!("Column {column1_name} not found in table {table1_name}");
    }
    if column2.is_none() {
        panic!("Column {column2_name} not found in table {table2_name}");
    }
    
    let table1 = table1.unwrap();
    let table2 = table2.unwrap();
    let column1 = column1.unwrap();
    let column2 = column2.unwrap();
    let mut best_method: String = String::from("Block Nested Join");
    let mut best_cost: u32 = block_nested_join_cost(&table1, &table2, memory_size);
    best_cost = match indexed_join_cost(&table1, &column1, &table2, &column2) {
        None => best_cost,
        Some(x) => {
            if x < best_cost {
                best_method = String::from("Indexed Join");
            }
            cmp::min(best_cost, x)
        }
    };

    let merge_cost = merge_join_cost(&table1, &column1, &table2, &column2, memory_size);
    if merge_cost < best_cost {
        best_cost = merge_cost;
        best_method = String::from("Merge Join");
    }
    best_cost = match hash_join_cost(&table1, &table2, memory_size) {
        None => best_cost,
        Some(x) => {
            if x < best_cost {
                best_method = String::from("Hash Join");
            }
            cmp::min(best_cost, x)
        }
    };

    println!("Memory size: {memory_size}");
    println!("User entered: {table1_name}.{column1_name} X {table2_name}.{column2_name}");

    println!("Best cost for joining is {best_cost} blocks by using method {best_method}");
}
