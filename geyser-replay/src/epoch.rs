use {
    crate::node::Kind,
    cid::Cid,
    std::{error::Error, vec::Vec},
};
// type (
// 	Epoch      struct {
// 		Kind    int
// 		Epoch   int
// 		Subsets List__Link
// 	}
// )
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Epoch {
    pub kind: u64,
    pub epoch: u64,
    pub subsets: Vec<Cid>,
}

impl Epoch {
    pub fn from_bytes(data: Vec<u8>) -> Result<Epoch, Box<dyn Error>> {
        let decoded_data: serde_cbor::Value = serde_cbor::from_slice(&data).unwrap();
        let epoch = Epoch::from_cbor(decoded_data)?;
        Ok(epoch)
    }

    pub fn from_cbor(val: serde_cbor::Value) -> Result<Epoch, Box<dyn Error>> {
        let mut epoch = Epoch {
            kind: 0,
            epoch: 0,
            subsets: vec![],
        };

        if let serde_cbor::Value::Array(array) = val {
            // println!("Kind: {:?}", array[0]);
            if let Some(serde_cbor::Value::Integer(kind)) = array.first() {
                // println!("Kind: {:?}", Kind::from_u64(kind as u64).unwrap().to_string());
                epoch.kind = *kind as u64;

                if *kind as u64 != Kind::Epoch as u64 {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        std::format!(
                            "Wrong kind for Epoch. Expected {:?}, got {:?}",
                            Kind::Epoch,
                            kind
                        ),
                    )));
                }
            }
            if let Some(serde_cbor::Value::Integer(num)) = array.get(1) {
                epoch.epoch = *num as u64;
            }

            if let Some(serde_cbor::Value::Array(subsets)) = &array.get(2) {
                for subset in subsets {
                    if let serde_cbor::Value::Bytes(subset) = subset {
                        epoch
                            .subsets
                            .push(Cid::try_from(subset[1..].to_vec()).unwrap());
                    }
                }
            }
        }
        Ok(epoch)
    }

    pub fn to_json(&self) -> serde_json::Value {
        let mut subsets = vec![];
        for subset in &self.subsets {
            subsets.push(serde_json::json!({
                "/": subset.to_string()
            }));
        }

        let mut map = serde_json::Map::new();
        map.insert("kind".to_string(), serde_json::Value::from(self.kind));
        map.insert("epoch".to_string(), serde_json::Value::from(self.epoch));
        map.insert("subsets".to_string(), serde_json::Value::from(subsets));

        serde_json::Value::from(map)
    }
}

#[cfg(test)]
mod epoch_tests {
    use super::*;

    #[test]
    fn test_epoch() {
        let epoch = Epoch {
            kind: 4,
            epoch: 1,
            subsets: vec![Cid::try_from(
                vec![
                    1, 113, 18, 32, 56, 148, 167, 251, 237, 117, 200, 226, 181, 134, 79, 115, 131,
                    220, 232, 143, 20, 67, 224, 179, 48, 130, 197, 123, 226, 85, 85, 56, 38, 84,
                    106, 225,
                ]
                .as_slice(),
            )
            .unwrap()],
        };
        let json = epoch.to_json();

        let wanted_json = serde_json::json!({
            "kind": 4,
            "epoch": 1,
            "subsets": [
                {
                    "/":"bafyreibysst7x3lvzdrllbspoob5z2epcrb6bmzqqlcxxysvku4cmvdk4e"
                }
            ]
        });

        assert_eq!(json, wanted_json);
    }

    #[test]
    fn test_decoding() {
        {
            let raw = vec![
                131, 4, 24, 39, 146, 216, 42, 88, 37, 0, 1, 113, 18, 32, 18, 250, 194, 194, 248,
                17, 163, 227, 226, 73, 89, 102, 172, 193, 238, 225, 98, 252, 63, 160, 136, 37, 67,
                188, 140, 158, 246, 249, 42, 240, 176, 158, 216, 42, 88, 37, 0, 1, 113, 18, 32,
                141, 232, 135, 32, 121, 0, 141, 52, 185, 135, 124, 244, 29, 48, 8, 213, 206, 34,
                160, 226, 133, 199, 250, 216, 46, 63, 127, 191, 1, 252, 193, 122, 216, 42, 88, 37,
                0, 1, 113, 18, 32, 28, 215, 1, 242, 11, 99, 190, 187, 29, 134, 111, 71, 180, 38,
                21, 233, 62, 146, 194, 176, 177, 47, 189, 174, 236, 78, 241, 30, 91, 101, 180, 22,
                216, 42, 88, 37, 0, 1, 113, 18, 32, 40, 118, 5, 84, 62, 143, 201, 110, 0, 235, 217,
                129, 120, 11, 135, 230, 60, 125, 28, 234, 31, 191, 19, 194, 9, 122, 240, 60, 68,
                178, 205, 177, 216, 42, 88, 37, 0, 1, 113, 18, 32, 189, 201, 201, 183, 204, 13,
                123, 108, 88, 63, 194, 26, 9, 177, 227, 158, 134, 213, 8, 206, 47, 165, 31, 23,
                191, 49, 108, 157, 153, 213, 131, 88, 216, 42, 88, 37, 0, 1, 113, 18, 32, 254, 223,
                153, 91, 142, 34, 11, 130, 186, 51, 189, 26, 251, 67, 219, 147, 144, 19, 162, 83,
                8, 82, 172, 15, 113, 200, 248, 28, 88, 91, 74, 164, 216, 42, 88, 37, 0, 1, 113, 18,
                32, 65, 102, 183, 74, 222, 146, 79, 191, 25, 96, 29, 218, 124, 17, 110, 46, 172,
                116, 33, 47, 27, 125, 80, 180, 164, 203, 127, 11, 28, 62, 206, 75, 216, 42, 88, 37,
                0, 1, 113, 18, 32, 167, 154, 154, 198, 222, 45, 240, 95, 86, 154, 251, 158, 68, 46,
                157, 230, 102, 187, 159, 103, 168, 114, 55, 109, 250, 44, 28, 71, 108, 82, 231,
                115, 216, 42, 88, 37, 0, 1, 113, 18, 32, 82, 71, 66, 71, 199, 27, 224, 128, 234,
                120, 160, 107, 143, 167, 64, 126, 207, 46, 72, 141, 134, 96, 90, 10, 157, 102, 84,
                129, 8, 99, 9, 56, 216, 42, 88, 37, 0, 1, 113, 18, 32, 10, 233, 51, 122, 206, 88,
                77, 159, 103, 28, 129, 195, 12, 115, 12, 107, 81, 146, 23, 193, 86, 41, 224, 121,
                37, 98, 65, 196, 222, 131, 123, 116, 216, 42, 88, 37, 0, 1, 113, 18, 32, 194, 151,
                126, 15, 113, 49, 181, 9, 67, 107, 40, 107, 192, 41, 213, 115, 233, 113, 14, 53,
                99, 130, 142, 127, 200, 225, 122, 46, 53, 48, 37, 56, 216, 42, 88, 37, 0, 1, 113,
                18, 32, 193, 11, 88, 188, 64, 8, 137, 103, 83, 62, 200, 254, 126, 250, 47, 140,
                116, 207, 16, 125, 221, 216, 119, 137, 156, 177, 209, 164, 48, 77, 166, 136, 216,
                42, 88, 37, 0, 1, 113, 18, 32, 161, 148, 55, 178, 229, 153, 194, 49, 141, 184, 223,
                219, 89, 53, 127, 213, 20, 255, 225, 254, 34, 26, 181, 198, 228, 166, 77, 8, 24,
                77, 68, 26, 216, 42, 88, 37, 0, 1, 113, 18, 32, 3, 144, 157, 93, 68, 243, 255, 185,
                75, 68, 156, 251, 18, 5, 206, 210, 83, 228, 52, 171, 254, 9, 69, 149, 9, 63, 91,
                217, 132, 15, 133, 42, 216, 42, 88, 37, 0, 1, 113, 18, 32, 124, 110, 193, 69, 202,
                85, 215, 41, 194, 150, 198, 245, 153, 132, 19, 9, 117, 110, 113, 30, 137, 231, 117,
                38, 211, 51, 154, 3, 125, 84, 52, 229, 216, 42, 88, 37, 0, 1, 113, 18, 32, 55, 34,
                35, 188, 88, 75, 147, 138, 231, 108, 17, 242, 53, 157, 170, 23, 90, 104, 245, 108,
                103, 181, 52, 108, 160, 67, 19, 245, 244, 196, 150, 170, 216, 42, 88, 37, 0, 1,
                113, 18, 32, 254, 72, 218, 251, 250, 18, 126, 94, 125, 102, 99, 110, 13, 94, 112,
                18, 52, 62, 65, 106, 155, 128, 69, 146, 21, 78, 103, 244, 129, 7, 176, 189, 216,
                42, 88, 37, 0, 1, 113, 18, 32, 44, 229, 44, 221, 134, 69, 72, 61, 15, 149, 152, 62,
                95, 52, 255, 190, 69, 44, 46, 188, 100, 36, 61, 165, 179, 54, 172, 131, 149, 143,
                143, 203,
            ];
            let as_json_raw = serde_json::json!({"kind":4,"epoch":39,"subsets":[{"/":"bafyreias7lbmf6arupr6eskzm2wmd3xbml6d7ieievb3zde6634sv4fqty"},{"/":"bafyreien5cdsa6iaru2ltb346qotacgvzyrkbyufy75nqlr7p67qd7gbpi"},{"/":"bafyreia424a7ec3dx25r3btpi62cmfpjh2jmfmfrf66253co6epfwznucy"},{"/":"bafyreibioycvipupzfxab26zqf4axb7ghr6rz2q7x4j4ecl26a6ejmwnwe"},{"/":"bafyreif5zhe3ptanpnwfqp6cdie3dy46q3kqrtrpuuprppzrnsoztvmdla"},{"/":"bafyreih636mvxdrcboblum55dl5uhw4tsaj2euyikkwa64oi7aofqw2kuq"},{"/":"bafyreicbm23uvxusj67rsya53j6bc3rovr2ccly3pviljjglp4frypwojm"},{"/":"bafyreifhtknmnxrn6bpvngx3tzcc5hpgm25z6z5ioi3w36rmdrdwyuxhom"},{"/":"bafyreicsi5bepry34caou6fanoh2oqd6z4xerdmgmbnavhlgksaqqyyjha"},{"/":"bafyreiak5ezxvtsyjwpwohebymghgddlkgjbpqkwfhqhsjlcihcn5a33oq"},{"/":"bafyreigcs57a64jrwueug2zinpactvlt5fyq4nldqkhh7shbpixdkmbfha"},{"/":"bafyreigbbnmlyqairftvgpwi7z7pul4mothra7o53b3ythfr2gsdatngra"},{"/":"bafyreifbsq33fzmzyiyy3og73nmtk76vct76d7rcdk24nzfgjuebqtkedi"},{"/":"bafyreiadscov2rht764uwre47mjaltwskpsdjk76bfczkcj7lpmyid4ffi"},{"/":"bafyreid4n3aulssv24u4ffwg6wmyieyjovxhchuj452snuzttibx2vbu4u"},{"/":"bafyreibxeir3ywclsofoo3ar6i2z3kqxljupk3dhwu2gzicdcp27jrewvi"},{"/":"bafyreih6jdnpx6qspzph2ztdnygv44asgq7ec2u3qbczefkom72icb5qxu"},{"/":"bafyreibm4uwn3bsfja6q7fmyhzptj756iuwc5pdeeq62lmzwvsbzld4pzm"}]});

            let epoch = Epoch::from_bytes(raw).unwrap();
            let json = epoch.to_json();

            assert_eq!(json, as_json_raw);
        }
        {
            let raw = vec![
                131, 4, 24, 120, 130, 216, 42, 88, 37, 0, 1, 113, 18, 32, 89, 118, 15, 47, 211,
                244, 148, 72, 97, 22, 125, 223, 7, 22, 154, 131, 239, 74, 68, 115, 25, 83, 181,
                103, 188, 221, 74, 184, 171, 49, 248, 175, 216, 42, 88, 37, 0, 1, 113, 18, 32, 111,
                243, 18, 145, 137, 92, 10, 252, 113, 31, 191, 162, 236, 105, 154, 211, 177, 143,
                180, 173, 61, 180, 154, 155, 60, 244, 221, 131, 213, 154, 68, 70,
            ];
            let as_json_raw = serde_json::json!({"kind":4,"epoch":120,"subsets":[{"/":"bafyreiczoyhs7u7usregcft534drngud55fei4yzko2wppg5jk4kwmpyv4"},{"/":"bafyreidp6mjjdck4bl6hch57ulwgtgwtwgh3jlj5wsnjwphu3wb5lgseiy"}]});

            let epoch = Epoch::from_bytes(raw).unwrap();
            let json = epoch.to_json();

            assert_eq!(json, as_json_raw);
        }
    }
}
