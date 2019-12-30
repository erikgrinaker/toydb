///! Evaluates SQL expressions and compares with expectations.
use super::super::types::Value;
use super::super::{Context, Engine, Parser, Plan, Transaction};
use crate::kv;
use crate::Error;

fn eval_expr(expr: &str) -> Result<Value, Error> {
    let engine = super::super::engine::KV::new(kv::MVCC::new(kv::storage::Memory::new()));
    let mut txn = engine.begin()?;
    let ctx = Context { txn: &mut txn };
    let ast = Parser::new(&format!("SELECT {}", expr)).parse()?;
    let mut result = Plan::build(ast)?.optimize()?.execute(ctx)?;
    let value = result.next().unwrap().unwrap().get(0).unwrap().clone();
    txn.rollback()?;
    Ok(value)
}

macro_rules! test_expr {
    ( $( $name:ident: $expr:expr => $result:expr, )* ) => {
    $(
        #[test]
        fn $name() -> Result<(), Error> {
            let value = eval_expr($expr);
            assert_eq!($result, value);
            Ok(())
        }
    )*
    }
}

use Value::*;

test_expr! {
    lit_bool_false: "FALSE" => Ok(Boolean(false)),
    lit_bool_true: "TRUE" => Ok(Boolean(true)),
    lit_float: "3.72" => Ok(Float(3.72)),
    lit_float_exp: "3.14e3" => Ok(Float(3140.0)),
    lit_float_exp_neg: "2.718E-2" => Ok(Float(0.02718)),
    lit_float_nodecimal: "3.0" => Ok(Float(3.0)),
    lit_int: "3" => Ok(Integer(3)),
    lit_int_multidigit: "314" => Ok(Integer(314)),
    lit_int_zeroprefix: "03" => Ok(Integer(3)),
    lit_null: "NULL" => Ok(Null),
    lit_str: "'Hi! 👋'" => Ok(String("Hi! 👋".into())),
    lit_str_quotes: r#"'Has ''single'' and "double" quotes'"# => Ok(String(r#"Has 'single' and "double" quotes"#.into())),

    op_add_float_float: "3.1 + 2.71" => Ok(Float(3.1 + 2.71)),
    op_add_float_int: "3.72 + 1" => Ok(Float(3.72 + 1.0)),
    op_add_float_null: "3.14 + NULL" => Ok(Null),
    op_add_int_float: "1 + 3.72" => Ok(Float(1.0 + 3.72)),
    op_add_int_int: "1 + 2" => Ok(Integer(3)),
    op_add_int_null: "1 + NULL" => Ok(Null),
    op_add_null_float: "NULL + 3.14" => Ok(Null),
    op_add_null_int: "NULL + 1" => Ok(Null),
    op_add_null_null: "NULL + NULL" => Ok(Null),
    op_add_negative: "1 + -3" => Ok(Integer(-2)),
    op_add_error_bool: "TRUE + FALSE" => Err(Error::Value("Can't add TRUE and FALSE".into())),
    op_add_error_strings: "'a' + 'b'" => Err(Error::Value("Can't add a and b".into())),

    op_and_true_true: "TRUE AND TRUE" => Ok(Boolean(true)),
    op_and_true_false: "TRUE AND FALSE" => Ok(Boolean(false)),
    op_and_false_true: "FALSE AND TRUE" => Ok(Boolean(false)),
    op_and_false_false: "FALSE AND FALSE" => Ok(Boolean(false)),
    op_and_true_null: "TRUE AND NULL" => Ok(Null),
    op_and_false_null: "FALSE AND NULL" => Ok(Boolean(false)),
    op_and_null_true: "NULL AND TRUE" => Ok(Null),
    op_and_null_false: "NULL AND FALSE" => Ok(Boolean(false)),
    op_and_null_null: "NULL AND NULL" => Ok(Null),
    op_and_error_float: "3.14 AND 3.14" => Err(Error::Value("Can't and 3.14 and 3.14".into())),
    op_and_error_integer: "3 AND 3" => Err(Error::Value("Can't and 3 and 3".into())),
    op_and_error_string: "'a' AND 'b'" => Err(Error::Value("Can't and a and b".into())),

    op_assert_float: "+3.72" => Ok(Float(3.72)),
    op_assert_int: "+1" => Ok(Integer(1)),
    op_assert_null: "+NULL" => Ok(Null),
    op_assert_multi: "+++1" => Ok(Integer(1)),
    op_assert_error_bool: "+TRUE" => Err(Error::Value("Can't take the positive of TRUE".into())),
    op_assert_error_string: "+'abc'" => Err(Error::Value("Can't take the positive of abc".into())),

    op_divide_float_float: "4.16 / 3.2" => Ok(Float(1.3)),
    op_divide_float_float_zero: "4.16 / 0.0" => Ok(Float(std::f64::INFINITY)),
    op_divide_float_integer: "1.5 / 3" => Ok(Float(0.5)),
    op_divide_float_integer_zero: "4.16 / 0" => Ok(Float(std::f64::INFINITY)),
    op_divide_float_null: "4.16 / NULL" => Ok(Null),
    op_divide_integer_float: "3 / 1.2" => Ok(Float(2.5)),
    op_divide_integer_float_zero: "3 / 0.0" => Ok(Float(std::f64::INFINITY)),
    op_divide_integer_integer: "8 / 3" => Ok(Integer(2)),
    op_divide_integer_integer_negative: "8 / -3" => Ok(Integer(-2)),
    op_divide_integer_null: "1 / NULL" => Ok(Null),
    op_divide_null_float: "NULL / 3.14" => Ok(Null),
    op_divide_null_integer: "NULL / 1" => Ok(Null),
    op_divide_null_null: "NULL / NULL" => Ok(Null),
    op_divide_error_bool: "TRUE / FALSE" => Err(Error::Value("Can't divide TRUE and FALSE".into())),
    op_divide_error_integer_zero: "1 / 0" => Err(Error::Value("Can't divide by zero".into())),
    op_divide_error_strings: "'a' / 'b'" => Err(Error::Value("Can't divide a and b".into())),

    op_eq_bool_false_false: "FALSE = FALSE" => Ok(Boolean(true)),
    op_eq_bool_true_false: "TRUE = FALSE" => Ok(Boolean(false)),
    op_eq_bool_true_true: "TRUE = TRUE" => Ok(Boolean(true)),
    op_eq_float: "3.14 = 3.14" => Ok(Boolean(true)),
    op_eq_float_neq: "3.14 = 2.718" => Ok(Boolean(false)),
    op_eq_float_int: "3.0 = 3" => Ok(Boolean(true)),
    op_eq_float_int_neq: "3.01 = 3" => Ok(Boolean(false)),
    op_eq_int: "1 = 1" => Ok(Boolean(true)),
    op_eq_int_neq: "1 = 2" => Ok(Boolean(false)),
    op_eq_str: "'abc' = 'abc'" => Ok(Boolean(true)),
    op_eq_str_case: "'abc' = 'ABC'" => Ok(Boolean(false)),
    op_eq_str_neq: "'abc' = 'xyz'" => Ok(Boolean(false)),

    op_exp_float_float: "6.25 ^ 0.5" => Ok(Float(2.5)),
    op_exp_float_int: "6.25 ^ 2" => Ok(Float(39.0625)),
    op_exp_float_null: "3.14 ^ NULL" => Ok(Null),
    op_exp_int_float: "9 ^ 0.5" => Ok(Float(3.0)),
    op_exp_int_int: "2 ^ 3" => Ok(Float(8.0)),
    op_exp_int_null: "1 ^ NULL" => Ok(Null),
    op_exp_null_float: "NULL ^ 3.14" => Ok(Null),
    op_exp_null_int: "NULL ^ 1" => Ok(Null),
    op_exp_null_null: "NULL ^ NULL" => Ok(Null),
    op_exp_negative: "2 ^ -3" => Ok(Float(0.125)),
    op_exp_error_bool: "TRUE ^ FALSE" => Err(Error::Value("Can't exponentiate TRUE and FALSE".into())),
    op_exp_error_strings: "'a' ^ 'b'" => Err(Error::Value("Can't exponentiate a and b".into())),

    op_factorial: "3!" => Ok(Integer(6)),
    op_factorial_null: "NULL!" => Ok(Null),
    op_factorial_error_bool: "TRUE!" => Err(Error::Value("Can't take factorial of TRUE".into())),
    op_factorial_error_float: "3.14!" => Err(Error::Value("Can't take factorial of 3.14".into())),
    // FIXME Can't fix this until we support parentheses
    //op_factorial_error_negative: "(-3)!" => Err(Error::Value("Can't take factorial of negative number".into())),
    op_factorial_error_string: "'abc'!" => Err(Error::Value("Can't take factorial of abc".into())),

    op_modulo_float_float: "6.28 % 2.2" => Ok(Float(1.88)),
    op_modulo_float_int: "3.15 % 2" => Ok(Float(1.15)),
    op_modulo_float_null: "3.14 % NULL" => Ok(Null),
    op_modulo_int_float: "6 % 3.15" => Ok(Float(2.85)),
    op_modulo_int_int: "7 % 3" => Ok(Integer(1)),
    op_modulo_int_null: "1 % NULL" => Ok(Null),
    op_modulo_null_float: "NULL % 3.14" => Ok(Null),
    op_modulo_null_int: "NULL % 1" => Ok(Null),
    op_modulo_null_null: "NULL % NULL" => Ok(Null),
    op_modulo_negative: "-5 % 3" => Ok(Integer(1)),
    op_modulo_negative_rhs: "5 % -3" => Ok(Integer(-1)),
    op_modulo_error_bool: "TRUE % FALSE" => Err(Error::Value("Can't take modulo of TRUE and FALSE".into())),
    op_modulo_error_strings: "'a' % 'b'" => Err(Error::Value("Can't take modulo of a and b".into())),

    op_multiply_float_float: "3.1 * 2.71" => Ok(Float(3.1 * 2.71)),
    op_multiply_float_int: "3.72 * 1" => Ok(Float(3.72 * 1.0)),
    op_multiply_float_null: "3.14 * NULL" => Ok(Null),
    op_multiply_int_float: "1 * 3.72" => Ok(Float(1.0 * 3.72)),
    op_multiply_int_int: "2 * 3" => Ok(Integer(6)),
    op_multiply_int_null: "1 * NULL" => Ok(Null),
    op_multiply_null_float: "NULL * 3.14" => Ok(Null),
    op_multiply_null_int: "NULL * 1" => Ok(Null),
    op_multiply_null_null: "NULL * NULL" => Ok(Null),
    op_multiply_negative: "1 * -3" => Ok(Integer(-3)),
    op_multiply_error_bool: "TRUE * FALSE" => Err(Error::Value("Can't multiply TRUE and FALSE".into())),
    op_multiply_error_strings: "'a' * 'b'" => Err(Error::Value("Can't multiply a and b".into())),

    op_negate: "-1" => Ok(Integer(-1)),
    op_negate_double: "--1" => Ok(Integer(1)),
    op_negate_float: "-3.72" => Ok(Float(-3.72)),
    op_negate_mixed: "-+-+-1" => Ok(Integer(-1)),
    op_negate_multi: "---1" => Ok(Integer(-1)),
    op_negate_null: "-NULL" => Ok(Null),
    op_negate_error_bool: "-TRUE" => Err(Error::Value("Can't negate TRUE".into())),
    op_negate_error_string: "-'abc'" => Err(Error::Value("Can't negate abc".into())),

    op_neq_bool_false_false: "FALSE != FALSE" => Ok(Boolean(false)),
    op_neq_bool_true_false: "TRUE != FALSE" => Ok(Boolean(true)),
    op_neq_bool_true_true: "TRUE != TRUE" => Ok(Boolean(false)),
    op_neq_float: "3.14 != 2.718" => Ok(Boolean(true)),
    op_neq_float_eq: "3.14 != 3.14" => Ok(Boolean(false)),
    op_neq_float_int: "3.01 != 3" => Ok(Boolean(true)),
    op_neq_float_int_eq: "3.0 != 3" => Ok(Boolean(false)),
    op_neq_int: "1 != 2" => Ok(Boolean(true)),
    op_neq_int_eq: "1 != 1" => Ok(Boolean(false)),
    op_neq_str: "'abc' != 'xyz'" => Ok(Boolean(true)),
    op_neq_str_case: "'abc' != 'ABC'" => Ok(Boolean(true)),
    op_neq_str_eq: "'abc' != 'abc'" => Ok(Boolean(false)),

    op_not_true: "NOT TRUE" => Ok(Boolean(false)),
    op_not_false: "NOT FALSE" => Ok(Boolean(true)),
    op_not_null: "NOT NULL" => Ok(Null),
    op_not_error_float: "NOT 3.14" => Err(Error::Value("Can't negate 3.14".into())),
    op_not_error_integer: "NOT 3" => Err(Error::Value("Can't negate 3".into())),
    op_not_error_string: "NOT 'abc'" => Err(Error::Value("Can't negate abc".into())),

    op_or_true_true: "TRUE OR TRUE" => Ok(Boolean(true)),
    op_or_true_false: "TRUE OR FALSE" => Ok(Boolean(true)),
    op_or_false_true: "FALSE OR TRUE" => Ok(Boolean(true)),
    op_or_false_false: "FALSE OR FALSE" => Ok(Boolean(false)),
    op_or_true_null: "TRUE OR NULL" => Ok(Boolean(true)),
    op_or_false_null: "FALSE OR NULL" => Ok(Null),
    op_or_null_true: "NULL OR TRUE" => Ok(Boolean(true)),
    op_or_null_false: "NULL OR FALSE" => Ok(Null),
    op_or_null_null: "NULL OR NULL" => Ok(Null),
    op_or_error_float: "3.14 OR 3.14" => Err(Error::Value("Can't or 3.14 and 3.14".into())),
    op_or_error_integer: "3 OR 3" => Err(Error::Value("Can't or 3 and 3".into())),
    op_or_error_string: "'a' OR 'b'" => Err(Error::Value("Can't or a and b".into())),

    op_subtract_float_float: "3.1 - 2.71" => Ok(Float(3.1 - 2.71)),
    op_subtract_float_int: "3.72 - 1" => Ok(Float(3.72 - 1.0)),
    op_subtract_float_null: "3.14 - NULL" => Ok(Null),
    op_subtract_int_float: "1 - 3.72" => Ok(Float(1.0 - 3.72)),
    op_subtract_int_int: "1 - 2" => Ok(Integer(-1)),
    op_subtract_int_null: "1 - NULL" => Ok(Null),
    op_subtract_null_float: "NULL - 3.14" => Ok(Null),
    op_subtract_null_int: "NULL - 1" => Ok(Null),
    op_subtract_null_null: "NULL - NULL" => Ok(Null),
    op_subtract_negative: "1 - -3" => Ok(Integer(4)),
    op_subtract_error_bool: "TRUE - FALSE" => Err(Error::Value("Can't subtract TRUE and FALSE".into())),
    op_subtract_error_strings: "'a' - 'b'" => Err(Error::Value("Can't subtract a and b".into())),
}
