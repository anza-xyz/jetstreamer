use {
    crate::{dataframe::DataFrame, node::Kind, utils::Buffer},
    bincode::deserialize,
    std::{error::Error, vec::Vec},
};

// type Transaction struct {
// 	Kind     int
// 	Data     DataFrame
// 	Metadata DataFrame
// 	Slot     int
// 	Index    **int
// }
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Transaction {
    pub kind: u64,
    pub data: DataFrame,
    pub metadata: DataFrame,
    pub slot: u64,
    pub index: Option<u64>,
}

impl Transaction {
    pub fn from_bytes(data: Vec<u8>) -> Result<Transaction, Box<dyn Error>> {
        let decoded_data: serde_cbor::Value = serde_cbor::from_slice(&data).unwrap();
        let transaction = Transaction::from_cbor(decoded_data)?;
        Ok(transaction)
    }

    // from serde_cbor::Value
    pub fn from_cbor(val: serde_cbor::Value) -> Result<Transaction, Box<dyn Error>> {
        let mut transaction = Transaction {
            kind: 0,
            data: DataFrame {
                kind: 0,
                hash: None,
                index: None,
                total: None,
                data: Buffer::new(),
                next: None,
            },
            metadata: DataFrame {
                kind: 0,
                hash: None,
                index: None,
                total: None,
                data: Buffer::new(),
                next: None,
            },
            slot: 0,
            index: None,
        };

        if let serde_cbor::Value::Array(array) = val {
            // println!("Kind: {:?}", array[0]);
            if let Some(serde_cbor::Value::Integer(kind)) = array.first() {
                // println!("Kind: {:?}", Kind::from_u64(kind as u64).unwrap().to_string());
                transaction.kind = *kind as u64;

                if *kind as u64 != Kind::Transaction as u64 {
                    return Err(Box::new(std::io::Error::other(std::format!(
                        "Wrong kind for Transaction; expected {:?}, got {:?}",
                        Kind::Transaction,
                        kind
                    ))));
                }
            }

            if let Some(serde_cbor::Value::Array(data)) = &array.get(1) {
                transaction.data = DataFrame::from_cbor(serde_cbor::Value::Array(data.clone()))?;
            }

            if let Some(serde_cbor::Value::Array(metadata)) = &array.get(2) {
                transaction.metadata =
                    DataFrame::from_cbor(serde_cbor::Value::Array(metadata.clone()))?;
            }

            if let Some(serde_cbor::Value::Integer(slot)) = array.get(3) {
                transaction.slot = *slot as u64;
            }

            if let Some(serde_cbor::Value::Integer(index)) = array.get(4) {
                transaction.index = Some(*index as u64);
            }
        }
        Ok(transaction)
    }

    pub fn to_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        map.insert("kind".to_string(), serde_json::Value::from(self.kind));
        map.insert("data".to_string(), self.data.to_json());
        map.insert("metadata".to_string(), self.metadata.to_json());
        map.insert("slot".to_string(), serde_json::Value::from(self.slot));
        map.insert("index".to_string(), serde_json::Value::from(self.index));

        serde_json::Value::from(map)
    }

    pub fn as_parsed(
        &self,
    ) -> Result<solana_sdk::transaction::VersionedTransaction, Box<dyn Error>> {
        Ok(deserialize(&self.data.data.to_vec())?)
    }

    /// Returns whether the transaction dataframe is complete or is split into multiple dataframes.
    pub fn is_complete_data(&self) -> bool {
        self.data.next.is_none() || self.data.next.as_ref().unwrap().is_empty()
    }
    /// Returns whether the transaction metadata is complete or is split into multiple dataframes.
    pub fn is_complete_metadata(&self) -> bool {
        self.metadata.next.is_none() || self.metadata.next.as_ref().unwrap().is_empty()
    }
}

#[cfg(test)]
mod transaction_tests {
    use {super::*, cid::Cid};

    #[test]
    fn test_transaction() {
        let transaction = Transaction {
            kind: 1,
            data: DataFrame {
                kind: 6,
                hash: Some(1),
                index: Some(1),
                total: Some(1),
                data: Buffer::from_vec(vec![1]),
                next: Some(vec![
                    Cid::try_from(
                        vec![
                            1, 113, 18, 32, 56, 148, 167, 251, 237, 117, 200, 226, 181, 134, 79,
                            115, 131, 220, 232, 143, 20, 67, 224, 179, 48, 130, 197, 123, 226, 85,
                            85, 56, 38, 84, 106, 225,
                        ]
                        .as_slice(),
                    )
                    .unwrap(),
                ]),
            },
            metadata: DataFrame {
                kind: 6,
                hash: Some(1),
                index: Some(1),
                total: Some(1),
                data: Buffer::from_vec(vec![1]),
                next: Some(vec![
                    Cid::try_from(
                        vec![
                            1, 113, 18, 32, 56, 148, 167, 251, 237, 117, 200, 226, 181, 134, 79,
                            115, 131, 220, 232, 143, 20, 67, 224, 179, 48, 130, 197, 123, 226, 85,
                            85, 56, 38, 84, 106, 225,
                        ]
                        .as_slice(),
                    )
                    .unwrap(),
                ]),
            },
            slot: 1,
            index: Some(1),
        };
        let json = transaction.to_json();

        let wanted_json = serde_json::json!({
            "kind": 1,
            "data": {
                "kind": 6,
                "hash": "1",
                "index": 1,
                "total": 1,
                "data": Buffer::from_vec(vec![1]).to_string(),
                "next": [{
                    "/":"bafyreibysst7x3lvzdrllbspoob5z2epcrb6bmzqqlcxxysvku4cmvdk4e"
                }]
            },
            "metadata": {
                "kind": 6,
                "hash": "1",
                "index": 1,
                "total": 1,
                "data": Buffer::from_vec(vec![1]).to_string(),
                "next": [{
                    "/":"bafyreibysst7x3lvzdrllbspoob5z2epcrb6bmzqqlcxxysvku4cmvdk4e"
                }]
            },
            "slot": 1,
            "index": 1
        });

        assert_eq!(json, wanted_json);
    }

    #[test]
    fn test_decoding() {
        {
            let raw = vec![
                133, 0, 133, 6, 246, 246, 246, 89, 1, 74, 1, 134, 211, 49, 71, 74, 192, 231, 203,
                60, 87, 178, 248, 12, 50, 114, 214, 129, 182, 44, 219, 155, 48, 56, 26, 34, 169,
                31, 8, 254, 225, 154, 223, 40, 155, 190, 199, 41, 122, 237, 248, 217, 3, 163, 103,
                212, 255, 27, 131, 158, 213, 220, 233, 238, 101, 89, 148, 91, 44, 124, 121, 34, 29,
                19, 8, 1, 0, 3, 5, 5, 25, 184, 120, 214, 101, 64, 179, 24, 204, 134, 159, 34, 65,
                196, 27, 118, 194, 159, 13, 31, 33, 150, 62, 102, 171, 127, 138, 217, 198, 46, 167,
                5, 25, 184, 108, 163, 149, 211, 120, 201, 249, 2, 7, 70, 58, 37, 139, 66, 81, 204,
                62, 85, 3, 238, 187, 182, 56, 109, 100, 146, 228, 35, 74, 6, 167, 213, 23, 25, 47,
                10, 175, 198, 242, 101, 227, 251, 119, 204, 122, 218, 130, 197, 41, 208, 190, 59,
                19, 110, 45, 0, 85, 32, 0, 0, 0, 6, 167, 213, 23, 24, 199, 116, 201, 40, 86, 99,
                152, 105, 29, 94, 182, 139, 94, 184, 163, 155, 75, 109, 92, 115, 85, 91, 33, 0, 0,
                0, 0, 7, 97, 72, 29, 53, 116, 116, 187, 124, 77, 118, 36, 235, 211, 189, 179, 216,
                53, 94, 115, 209, 16, 67, 252, 13, 163, 83, 128, 0, 0, 0, 0, 182, 60, 207, 33, 158,
                150, 214, 144, 149, 162, 94, 67, 156, 12, 11, 6, 76, 240, 19, 151, 216, 246, 121,
                45, 88, 34, 202, 217, 240, 232, 241, 11, 1, 4, 4, 1, 2, 3, 0, 61, 2, 0, 0, 0, 2, 0,
                0, 0, 0, 0, 0, 0, 125, 20, 1, 1, 0, 0, 0, 0, 126, 20, 1, 1, 0, 0, 0, 0, 242, 171,
                7, 179, 147, 12, 194, 246, 147, 38, 135, 62, 250, 65, 130, 82, 252, 134, 159, 218,
                29, 218, 191, 18, 122, 23, 147, 40, 41, 53, 184, 88, 0, 133, 6, 246, 246, 246, 88,
                59, 40, 181, 47, 253, 4, 0, 117, 1, 0, 34, 66, 7, 16, 208, 71, 1, 63, 61, 210, 40,
                159, 253, 19, 122, 41, 43, 143, 242, 125, 96, 156, 189, 165, 133, 94, 14, 17, 234,
                253, 193, 124, 5, 0, 167, 122, 8, 50, 94, 65, 214, 206, 28, 106, 40, 95, 237, 237,
                196, 226, 26, 1, 1, 20, 132, 0,
            ];
            let as_json_raw = serde_json::json!({"kind":0,"data":{"kind":6,"hash":null,"index":null,"total":null,"data":"AYbTMUdKwOfLPFey+AwyctaBtizbmzA4GiKpHwj+4ZrfKJu+xyl67fjZA6Nn1P8bg57V3OnuZVmUWyx8eSIdEwgBAAMFBRm4eNZlQLMYzIafIkHEG3bCnw0fIZY+Zqt/itnGLqcFGbhso5XTeMn5AgdGOiWLQlHMPlUD7ru2OG1kkuQjSgan1RcZLwqvxvJl4/t3zHragsUp0L47E24tAFUgAAAABqfVFxjHdMkoVmOYaR1etoteuKObS21cc1VbIQAAAAAHYUgdNXR0u3xNdiTr072z2DVec9EQQ/wNo1OAAAAAALY8zyGeltaQlaJeQ5wMCwZM8BOX2PZ5LVgiytnw6PELAQQEAQIDAD0CAAAAAgAAAAAAAAB9FAEBAAAAAH4UAQEAAAAA8qsHs5MMwvaTJoc++kGCUvyGn9od2r8SeheTKCk1uFgA","next":null},"metadata":{"kind":6,"hash":null,"index":null,"total":null,"data":"KLUv/QQAdQEAIkIHENBHAT890iif/RN6KSuP8n1gnL2lhV4OEer9wXwFAKd6CDJeQdbOHGooX+3txOI=","next":null},"slot":16848004,"index":0});

            let transaction = Transaction::from_bytes(raw).unwrap();
            let as_json = transaction.to_json();
            assert_eq!(as_json, as_json_raw);
        }
        {
            let raw = vec![
                133, 0, 133, 6, 246, 246, 246, 89, 1, 66, 1, 151, 159, 89, 187, 97, 25, 142, 3,
                174, 85, 157, 116, 102, 197, 178, 214, 246, 74, 226, 141, 31, 105, 16, 37, 67, 105,
                225, 141, 254, 92, 224, 101, 93, 67, 251, 238, 169, 227, 57, 40, 109, 130, 196,
                111, 38, 164, 190, 143, 138, 201, 237, 155, 1, 79, 81, 30, 199, 180, 46, 87, 224,
                244, 40, 10, 1, 0, 3, 5, 172, 22, 10, 112, 218, 101, 149, 13, 246, 88, 186, 12, 9,
                221, 143, 104, 189, 65, 202, 38, 214, 139, 78, 84, 16, 83, 141, 70, 208, 142, 246,
                211, 127, 174, 161, 97, 171, 234, 188, 35, 150, 54, 103, 237, 9, 22, 182, 119, 200,
                88, 156, 56, 108, 149, 249, 232, 100, 47, 132, 163, 172, 119, 226, 37, 6, 167, 213,
                23, 25, 47, 10, 175, 198, 242, 101, 227, 251, 119, 204, 122, 218, 130, 197, 41,
                208, 190, 59, 19, 110, 45, 0, 85, 32, 0, 0, 0, 6, 167, 213, 23, 24, 199, 116, 201,
                40, 86, 99, 152, 105, 29, 94, 182, 139, 94, 184, 163, 155, 75, 109, 92, 115, 85,
                91, 33, 0, 0, 0, 0, 7, 97, 72, 29, 53, 116, 116, 187, 124, 77, 118, 36, 235, 211,
                189, 179, 216, 53, 94, 115, 209, 16, 67, 252, 13, 163, 83, 128, 0, 0, 0, 0, 4, 201,
                29, 212, 80, 118, 182, 160, 37, 251, 217, 53, 53, 233, 25, 246, 252, 227, 101, 151,
                134, 14, 148, 250, 179, 200, 158, 39, 252, 116, 174, 37, 1, 4, 4, 1, 2, 3, 0, 53,
                2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 127, 20, 1, 1, 0, 0, 0, 0, 34, 140, 235, 141,
                252, 67, 143, 16, 185, 136, 66, 108, 201, 186, 4, 240, 250, 90, 51, 224, 222, 196,
                82, 242, 30, 110, 233, 49, 110, 196, 49, 109, 0, 133, 6, 246, 246, 246, 88, 60, 40,
                181, 47, 253, 4, 0, 125, 1, 0, 34, 130, 7, 17, 224, 73, 1, 128, 250, 173, 82, 174,
                11, 170, 74, 29, 145, 65, 49, 190, 15, 10, 157, 189, 157, 100, 62, 2, 89, 226, 253,
                160, 124, 5, 0, 167, 122, 8, 50, 94, 65, 214, 206, 28, 106, 40, 95, 16, 123, 220,
                102, 26, 1, 1, 20, 132, 6,
            ];
            let as_json_raw = serde_json::json!({"kind":0,"data":{"kind":6,"hash":null,"index":null,"total":null,"data":"AZefWbthGY4DrlWddGbFstb2SuKNH2kQJUNp4Y3+XOBlXUP77qnjOShtgsRvJqS+j4rJ7ZsBT1Eex7QuV+D0KAoBAAMFrBYKcNpllQ32WLoMCd2PaL1ByibWi05UEFONRtCO9tN/rqFhq+q8I5Y2Z+0JFrZ3yFicOGyV+ehkL4SjrHfiJQan1RcZLwqvxvJl4/t3zHragsUp0L47E24tAFUgAAAABqfVFxjHdMkoVmOYaR1etoteuKObS21cc1VbIQAAAAAHYUgdNXR0u3xNdiTr072z2DVec9EQQ/wNo1OAAAAAAATJHdRQdragJfvZNTXpGfb842WXhg6U+rPInif8dK4lAQQEAQIDADUCAAAAAQAAAAAAAAB/FAEBAAAAACKM6438Q48QuYhCbMm6BPD6WjPg3sRS8h5u6TFuxDFtAA==","next":null},"metadata":{"kind":6,"hash":null,"index":null,"total":null,"data":"KLUv/QQAfQEAIoIHEeBJAYD6rVKuC6pKHZFBMb4PCp29nWQ+Alni/aB8BQCneggyXkHWzhxqKF8Qe9xm","next":null},"slot":16848004,"index":6});

            let transaction = Transaction::from_bytes(raw).unwrap();
            let as_json = transaction.to_json();
            assert_eq!(as_json, as_json_raw);
        }
        {
            let raw = vec![
                133, 0, 133, 6, 246, 246, 246, 89, 1, 74, 1, 77, 56, 38, 7, 194, 192, 28, 222, 51,
                93, 37, 184, 107, 166, 11, 163, 39, 199, 194, 22, 136, 238, 106, 134, 241, 210,
                230, 111, 82, 132, 58, 57, 182, 245, 103, 20, 212, 127, 136, 207, 86, 78, 145, 44,
                95, 194, 150, 52, 180, 58, 22, 60, 38, 119, 51, 173, 193, 149, 101, 36, 50, 80, 53,
                14, 1, 0, 3, 5, 190, 70, 100, 24, 253, 30, 159, 110, 80, 154, 11, 229, 134, 11, 97,
                240, 128, 102, 162, 236, 119, 116, 81, 222, 200, 65, 22, 104, 192, 248, 4, 36, 238,
                79, 232, 183, 174, 31, 1, 233, 191, 201, 171, 51, 122, 73, 184, 10, 99, 218, 1, 71,
                77, 136, 192, 226, 244, 4, 5, 41, 165, 165, 43, 177, 6, 167, 213, 23, 25, 47, 10,
                175, 198, 242, 101, 227, 251, 119, 204, 122, 218, 130, 197, 41, 208, 190, 59, 19,
                110, 45, 0, 85, 32, 0, 0, 0, 6, 167, 213, 23, 24, 199, 116, 201, 40, 86, 99, 152,
                105, 29, 94, 182, 139, 94, 184, 163, 155, 75, 109, 92, 115, 85, 91, 33, 0, 0, 0, 0,
                7, 97, 72, 29, 53, 116, 116, 187, 124, 77, 118, 36, 235, 211, 189, 179, 216, 53,
                94, 115, 209, 16, 67, 252, 13, 163, 83, 128, 0, 0, 0, 0, 182, 60, 207, 33, 158,
                150, 214, 144, 149, 162, 94, 67, 156, 12, 11, 6, 76, 240, 19, 151, 216, 246, 121,
                45, 88, 34, 202, 217, 240, 232, 241, 11, 1, 4, 4, 1, 2, 3, 0, 61, 2, 0, 0, 0, 2, 0,
                0, 0, 0, 0, 0, 0, 125, 20, 1, 1, 0, 0, 0, 0, 126, 20, 1, 1, 0, 0, 0, 0, 242, 171,
                7, 179, 147, 12, 194, 246, 147, 38, 135, 62, 250, 65, 130, 82, 252, 134, 159, 218,
                29, 218, 191, 18, 122, 23, 147, 40, 41, 53, 184, 88, 0, 133, 6, 246, 246, 246, 88,
                60, 40, 181, 47, 253, 4, 0, 125, 1, 0, 34, 130, 7, 17, 208, 71, 1, 15, 126, 161,
                171, 215, 190, 136, 255, 30, 141, 212, 35, 124, 31, 104, 156, 189, 29, 101, 78,
                130, 72, 235, 253, 160, 124, 5, 0, 167, 122, 8, 50, 94, 65, 214, 206, 28, 106, 40,
                95, 22, 54, 11, 168, 26, 1, 1, 20, 132, 8,
            ];
            let as_json_raw = serde_json::json!({"kind":0,"data":{"kind":6,"hash":null,"index":null,"total":null,"data":"AU04JgfCwBzeM10luGumC6Mnx8IWiO5qhvHS5m9ShDo5tvVnFNR/iM9WTpEsX8KWNLQ6FjwmdzOtwZVlJDJQNQ4BAAMFvkZkGP0en25Qmgvlhgth8IBmoux3dFHeyEEWaMD4BCTuT+i3rh8B6b/JqzN6SbgKY9oBR02IwOL0BAUppaUrsQan1RcZLwqvxvJl4/t3zHragsUp0L47E24tAFUgAAAABqfVFxjHdMkoVmOYaR1etoteuKObS21cc1VbIQAAAAAHYUgdNXR0u3xNdiTr072z2DVec9EQQ/wNo1OAAAAAALY8zyGeltaQlaJeQ5wMCwZM8BOX2PZ5LVgiytnw6PELAQQEAQIDAD0CAAAAAgAAAAAAAAB9FAEBAAAAAH4UAQEAAAAA8qsHs5MMwvaTJoc++kGCUvyGn9od2r8SeheTKCk1uFgA","next":null},"metadata":{"kind":6,"hash":null,"index":null,"total":null,"data":"KLUv/QQAfQEAIoIHEdBHAQ9+oavXvoj/Ho3UI3wfaJy9HWVOgkjr/aB8BQCneggyXkHWzhxqKF8WNguo","next":null},"slot":16848004,"index":8});

            let transaction = Transaction::from_bytes(raw).unwrap();
            let as_json = transaction.to_json();
            assert_eq!(as_json, as_json_raw);
        }
        {
            let raw = vec![
                133, 0, 133, 6, 246, 246, 246, 89, 1, 74, 1, 184, 225, 58, 101, 82, 111, 167, 65,
                53, 254, 197, 113, 213, 145, 193, 123, 203, 12, 233, 149, 120, 43, 195, 116, 126,
                44, 173, 8, 91, 41, 28, 35, 213, 132, 158, 203, 27, 161, 167, 40, 32, 77, 153, 112,
                239, 76, 170, 93, 5, 252, 225, 83, 56, 16, 16, 186, 219, 240, 67, 87, 114, 170, 53,
                0, 1, 0, 3, 5, 172, 22, 10, 112, 218, 101, 149, 13, 246, 88, 186, 12, 9, 221, 143,
                104, 189, 65, 202, 38, 214, 139, 78, 84, 16, 83, 141, 70, 208, 142, 246, 211, 127,
                174, 161, 97, 171, 234, 188, 35, 150, 54, 103, 237, 9, 22, 182, 119, 200, 88, 156,
                56, 108, 149, 249, 232, 100, 47, 132, 163, 172, 119, 226, 37, 6, 167, 213, 23, 25,
                47, 10, 175, 198, 242, 101, 227, 251, 119, 204, 122, 218, 130, 197, 41, 208, 190,
                59, 19, 110, 45, 0, 85, 32, 0, 0, 0, 6, 167, 213, 23, 24, 199, 116, 201, 40, 86,
                99, 152, 105, 29, 94, 182, 139, 94, 184, 163, 155, 75, 109, 92, 115, 85, 91, 33, 0,
                0, 0, 0, 7, 97, 72, 29, 53, 116, 116, 187, 124, 77, 118, 36, 235, 211, 189, 179,
                216, 53, 94, 115, 209, 16, 67, 252, 13, 163, 83, 128, 0, 0, 0, 0, 57, 115, 227, 48,
                194, 155, 131, 31, 63, 203, 14, 73, 55, 78, 216, 208, 56, 143, 65, 10, 35, 228,
                235, 242, 51, 40, 80, 80, 54, 239, 189, 3, 1, 4, 4, 1, 2, 3, 0, 61, 2, 0, 0, 0, 1,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 171, 3, 64, 92, 84, 205, 196, 47, 165,
                30, 214, 130, 189, 56, 19, 137, 214, 2, 67, 245, 115, 151, 196, 222, 30, 118, 173,
                83, 211, 213, 98, 74, 1, 155, 141, 111, 94, 0, 0, 0, 0, 133, 6, 246, 246, 246, 64,
                1, 1,
            ];
            let as_json_raw = serde_json::json!({"kind":0,"data":{"kind":6,"hash":null,"index":null,"total":null,"data":"AbjhOmVSb6dBNf7FcdWRwXvLDOmVeCvDdH4srQhbKRwj1YSeyxuhpyggTZlw70yqXQX84VM4EBC62/BDV3KqNQABAAMFrBYKcNpllQ32WLoMCd2PaL1ByibWi05UEFONRtCO9tN/rqFhq+q8I5Y2Z+0JFrZ3yFicOGyV+ehkL4SjrHfiJQan1RcZLwqvxvJl4/t3zHragsUp0L47E24tAFUgAAAABqfVFxjHdMkoVmOYaR1etoteuKObS21cc1VbIQAAAAAHYUgdNXR0u3xNdiTr072z2DVec9EQQ/wNo1OAAAAAADlz4zDCm4MfP8sOSTdO2NA4j0EKI+Tr8jMoUFA2770DAQQEAQIDAD0CAAAAAQAAAAAAAAAAAAAAAAAAAKsDQFxUzcQvpR7Wgr04E4nWAkP1c5fE3h52rVPT1WJKAZuNb14AAAAA","next":null},"metadata":{"kind":6,"hash":null,"index":null,"total":null,"data":"","next":null},"slot":1,"index":1});

            let transaction = Transaction::from_bytes(raw).unwrap();
            let as_json = transaction.to_json();
            assert_eq!(as_json, as_json_raw);
        }
        {
            let raw = vec![
                133, 0, 133, 6, 246, 246, 246, 89, 1, 74, 1, 209, 218, 80, 205, 183, 226, 44, 58,
                188, 80, 169, 129, 20, 90, 23, 130, 239, 173, 189, 172, 2, 98, 168, 145, 31, 193,
                131, 54, 144, 44, 133, 91, 197, 203, 77, 135, 52, 132, 228, 140, 123, 66, 190, 138,
                193, 104, 202, 136, 198, 60, 159, 228, 136, 161, 97, 79, 183, 181, 202, 219, 184,
                16, 210, 2, 1, 0, 3, 5, 8, 174, 144, 179, 253, 128, 62, 129, 35, 232, 153, 1, 56,
                61, 76, 245, 77, 47, 140, 172, 72, 99, 201, 10, 175, 163, 76, 80, 69, 184, 105,
                195, 8, 174, 144, 179, 221, 8, 189, 75, 88, 135, 173, 62, 74, 163, 208, 136, 15,
                182, 90, 121, 92, 255, 108, 230, 47, 143, 61, 249, 76, 92, 69, 116, 6, 167, 213,
                23, 25, 47, 10, 175, 198, 242, 101, 227, 251, 119, 204, 122, 218, 130, 197, 41,
                208, 190, 59, 19, 110, 45, 0, 85, 32, 0, 0, 0, 6, 167, 213, 23, 24, 199, 116, 201,
                40, 86, 99, 152, 105, 29, 94, 182, 139, 94, 184, 163, 155, 75, 109, 92, 115, 85,
                91, 33, 0, 0, 0, 0, 7, 97, 72, 29, 53, 116, 116, 187, 124, 77, 118, 36, 235, 211,
                189, 179, 216, 53, 94, 115, 209, 16, 67, 252, 13, 163, 83, 128, 0, 0, 0, 0, 182,
                60, 207, 33, 158, 150, 214, 144, 149, 162, 94, 67, 156, 12, 11, 6, 76, 240, 19,
                151, 216, 246, 121, 45, 88, 34, 202, 217, 240, 232, 241, 11, 1, 4, 4, 1, 2, 3, 0,
                61, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 125, 20, 1, 1, 0, 0, 0, 0, 126, 20, 1, 1,
                0, 0, 0, 0, 242, 171, 7, 179, 147, 12, 194, 246, 147, 38, 135, 62, 250, 65, 130,
                82, 252, 134, 159, 218, 29, 218, 191, 18, 122, 23, 147, 40, 41, 53, 184, 88, 0,
                133, 6, 246, 246, 246, 88, 59, 40, 181, 47, 253, 4, 0, 117, 1, 0, 34, 66, 7, 16,
                224, 73, 1, 0, 168, 9, 24, 157, 82, 90, 181, 218, 67, 81, 191, 15, 2, 88, 125, 157,
                116, 39, 200, 140, 107, 127, 40, 31, 5, 0, 167, 122, 8, 50, 94, 65, 214, 206, 28,
                106, 40, 95, 143, 10, 67, 190, 26, 1, 1, 20, 132, 1,
            ];
            let as_json_raw = serde_json::json!({"kind":0,"data":{"kind":6,"hash":null,"index":null,"total":null,"data":"AdHaUM234iw6vFCpgRRaF4Lvrb2sAmKokR/BgzaQLIVbxctNhzSE5Ix7Qr6KwWjKiMY8n+SIoWFPt7XK27gQ0gIBAAMFCK6Qs/2APoEj6JkBOD1M9U0vjKxIY8kKr6NMUEW4acMIrpCz3Qi9S1iHrT5Ko9CID7ZaeVz/bOYvjz35TFxFdAan1RcZLwqvxvJl4/t3zHragsUp0L47E24tAFUgAAAABqfVFxjHdMkoVmOYaR1etoteuKObS21cc1VbIQAAAAAHYUgdNXR0u3xNdiTr072z2DVec9EQQ/wNo1OAAAAAALY8zyGeltaQlaJeQ5wMCwZM8BOX2PZ5LVgiytnw6PELAQQEAQIDAD0CAAAAAgAAAAAAAAB9FAEBAAAAAH4UAQEAAAAA8qsHs5MMwvaTJoc++kGCUvyGn9od2r8SeheTKCk1uFgA","next":null},"metadata":{"kind":6,"hash":null,"index":null,"total":null,"data":"KLUv/QQAdQEAIkIHEOBJAQCoCRidUlq12kNRvw8CWH2ddCfIjGt/KB8FAKd6CDJeQdbOHGooX48KQ74=","next":null},"slot":16848004,"index":1});

            let transaction = Transaction::from_bytes(raw).unwrap();
            let as_json = transaction.to_json();
            assert_eq!(as_json, as_json_raw);
        }
        {
            let raw = vec![
                133, 0, 133, 6, 246, 246, 246, 89, 1, 82, 1, 7, 129, 215, 180, 55, 12, 107, 0, 191,
                100, 122, 6, 102, 204, 238, 233, 26, 95, 38, 50, 157, 117, 102, 175, 231, 40, 105,
                159, 211, 41, 252, 138, 221, 248, 201, 176, 68, 46, 213, 242, 96, 239, 1, 13, 247,
                199, 59, 15, 227, 127, 42, 144, 68, 138, 39, 148, 186, 108, 159, 69, 202, 35, 166,
                2, 1, 0, 3, 5, 25, 186, 124, 248, 30, 85, 38, 82, 76, 137, 213, 19, 241, 20, 187,
                124, 55, 101, 45, 215, 64, 18, 62, 67, 242, 195, 34, 238, 13, 131, 155, 166, 178,
                221, 184, 16, 109, 186, 103, 212, 50, 177, 183, 25, 134, 20, 39, 250, 37, 111, 219,
                217, 104, 215, 137, 162, 222, 110, 196, 196, 148, 168, 35, 45, 6, 167, 213, 23, 25,
                47, 10, 175, 198, 242, 101, 227, 251, 119, 204, 122, 218, 130, 197, 41, 208, 190,
                59, 19, 110, 45, 0, 85, 32, 0, 0, 0, 6, 167, 213, 23, 24, 199, 116, 201, 40, 86,
                99, 152, 105, 29, 94, 182, 139, 94, 184, 163, 155, 75, 109, 92, 115, 85, 91, 33, 0,
                0, 0, 0, 7, 97, 72, 29, 53, 116, 116, 187, 124, 77, 118, 36, 235, 211, 189, 179,
                216, 53, 94, 115, 209, 16, 67, 252, 13, 163, 83, 128, 0, 0, 0, 0, 4, 201, 29, 212,
                80, 118, 182, 160, 37, 251, 217, 53, 53, 233, 25, 246, 252, 227, 101, 151, 134, 14,
                148, 250, 179, 200, 158, 39, 252, 116, 174, 37, 1, 4, 4, 1, 2, 3, 0, 69, 2, 0, 0,
                0, 3, 0, 0, 0, 0, 0, 0, 0, 125, 20, 1, 1, 0, 0, 0, 0, 126, 20, 1, 1, 0, 0, 0, 0,
                127, 20, 1, 1, 0, 0, 0, 0, 34, 140, 235, 141, 252, 67, 143, 16, 185, 136, 66, 108,
                201, 186, 4, 240, 250, 90, 51, 224, 222, 196, 82, 242, 30, 110, 233, 49, 110, 196,
                49, 109, 0, 133, 6, 246, 246, 246, 88, 59, 40, 181, 47, 253, 4, 0, 117, 1, 0, 34,
                66, 7, 16, 224, 73, 1, 56, 247, 104, 106, 157, 235, 62, 6, 214, 24, 10, 249, 125,
                129, 88, 125, 153, 83, 62, 14, 137, 212, 126, 112, 31, 5, 0, 167, 122, 8, 50, 94,
                65, 214, 206, 28, 106, 40, 95, 20, 171, 252, 8, 26, 1, 1, 20, 132, 4,
            ];
            let as_json_raw = serde_json::json!({"kind":0,"data":{"kind":6,"hash":null,"index":null,"total":null,"data":"AQeB17Q3DGsAv2R6BmbM7ukaXyYynXVmr+coaZ/TKfyK3fjJsEQu1fJg7wEN98c7D+N/KpBEiieUumyfRcojpgIBAAMFGbp8+B5VJlJMidUT8RS7fDdlLddAEj5D8sMi7g2Dm6ay3bgQbbpn1DKxtxmGFCf6JW/b2WjXiaLebsTElKgjLQan1RcZLwqvxvJl4/t3zHragsUp0L47E24tAFUgAAAABqfVFxjHdMkoVmOYaR1etoteuKObS21cc1VbIQAAAAAHYUgdNXR0u3xNdiTr072z2DVec9EQQ/wNo1OAAAAAAATJHdRQdragJfvZNTXpGfb842WXhg6U+rPInif8dK4lAQQEAQIDAEUCAAAAAwAAAAAAAAB9FAEBAAAAAH4UAQEAAAAAfxQBAQAAAAAijOuN/EOPELmIQmzJugTw+loz4N7EUvIebukxbsQxbQA=","next":null},"metadata":{"kind":6,"hash":null,"index":null,"total":null,"data":"KLUv/QQAdQEAIkIHEOBJATj3aGqd6z4G1hgK+X2BWH2ZUz4OidR+cB8FAKd6CDJeQdbOHGooXxSr/Ag=","next":null},"slot":16848004,"index":4});

            let transaction = Transaction::from_bytes(raw).unwrap();
            let as_json = transaction.to_json();
            assert_eq!(as_json, as_json_raw);
        }
    }
}
