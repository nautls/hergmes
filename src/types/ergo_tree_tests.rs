use super::*;
use crate::fixtures;

/// Every constant in Fleet SDK's `constantVectors.ts` (fleet-sdk/fleet 8fd834a,
/// `packages/serializer/src/_test-vectors`), Box constants aside.
const FLEET_CONSTANTS: &[&str] = &[
    "0101",
    "0100",
    "0201",
    "0202",
    "024c",
    "027f",
    "0280",
    "0302",
    "0303",
    "0322",
    "03feff03",
    "03ffff03",
    "0400",
    "0401",
    "0402",
    "0405",
    "0414",
    "042d",
    "04800f",
    "04808008",
    "04808023",
    "04feffffffffffffffff01",
    "04ffffffffffffffffff01",
    "0500",
    "0501",
    "0502",
    "0503",
    "0513",
    "05800f",
    "05800f",
    "05807d",
    "05808084af5f",
    "05808084af5f",
    "05808087a70e",
    "058080808ae213",
    "0580808ca1fa60",
    "05808091f3ad04",
    "0580808bd0fda201",
    "0580808bad9bc718",
    "0580808592e78878",
    "0580809d80d0bf9901",
    "058080cba684a68201",
    "058080b4ccd4dfc603",
    "058080a0f6f4acdbe01b",
    "05feffffffffffffffff01",
    "05ffffffffffffffffff01",
    "060104",
    "06011a",
    "06017a",
    "0602040f",
    "060400c1f4a5",
    "060519debd01c7",
    "06060102b36211c7",
    "060f9c2404f2634ef40afccc320eed30b3",
    "060fa48aae0a58c333fcb45c22eb77423f",
    "060fdd1c7569871f783f9e7c9489215a93",
    "060ff0be3a08b885a5c09b96aee8797fef",
    "060fb5c30093fd377e9986d2cb46a6aa80",
    "060fcbc5a938f002f37a8784120c51000e",
    "06100080c2a6bf0e86c4b39aaf5b72bf25cf",
    "061000844935789da1a1015e411873fd04a3",
    "06100085a44d9646705fedbf146bcd80d8ef",
    "061000c04e5372d5aec10adf40f09f9695ab",
    "061000c1152c6ecd3042dd7af6d46b4c5e9f",
    "061000fb98488ddf56996aeb9434f5803b8f",
    "061000fec346162748f0e3c268260919f73f",
    "061000ff0aed27d915df669709f890468527",
    "06104b3b4ca85a86c47a098a223fffffffff",
    "0610fd399ad011430f3cf1173d88e0157d88",
    "0610efcb7de64abd55fa4a07b00ffcc80e04",
    "0610f7ee0f9e6b31e5b8bf79c0103cd98a94",
    "0610e568d66fc6ef02311087197a516bfd08",
    "0610ea4b448df22c343557e9d34980ecd344",
    "0610ed8d2982d73cf954ca90f09eebd46697",
    "0610f6a12915ff35fbfdc0de719c2b44239f",
    "0610e730b99d5580f36594530fd961844c3d",
    "0610f51c7da6efcc5267e609d013e6464820",
    "0610eb5839c44ec0be3d5b95d4c9522d72ca",
    "0610f7ce9e56ef64e15237a2ab2d91aa1f6b",
    "0610d3d187be808cd61d4215be07445d3179",
    "0610edad8f7090c4b7782c4d8e563c3e731c",
    "0610f34e21b2b957ed470b2baabc227286a0",
    "0610da31d4846523942d30bf81311ed324e8",
    "0610eee296791bc9b5bc269844845d3d5eec",
    "0610f43d7dea14e4619a1e2e43774e908e20",
    "0610ec6afd4980220003c03b50fee83e697f",
    "061913aaf504e4bc1e62173f87a4378c37b49c8ccff196ce3f0ad2",
    "07000000000000000000000000000000000000000000000000000000000000000000",
    "0702000031a06023f7d372f748a816db1765b4e4f1989cf89791c021a37ce09dae37",
    "0702000574ed4686be3f2be5a345f48774039820a2a830efd3d8f2cecb6ec28901b6",
    "08cd0200205e052506a4bc22dd580b721bfa994fe954ada92e8e9469636b47f57d9477",
    "08cd0200a4bffdc39c74ce1f31346dd4f47795b9dcd8cd256aab68828a4582e84d4c0e",
    "08cd0200c662d546939237a0195ef8be81fb0f939285c374b3589cc5d7172c98e33b22",
    "0d0c010e",
    "0d0c0101",
    "0d400e01020a0b010202",
    "0e0a46656d616c6520233035",
    "0e0130",
    "0e00",
    "0efe0b476f6c6420686173206265656e20612073746f7265206f66207765616c746820666f722074686f7573616e6473206f6620796561727320616e6420686173206f75746c6173746564207468652063757272656e63696573206f6620766172696f757320656d706972657320616e64206e6174696f6e2d7374617465732074686174206861766520636f6d6520616e6420676f6e652e20497420697320657374696d6174656420746861742077652068617665206c657373207468616e2035352c30303020746f6e73206f6620676f6c64206c65667420746f20646973636f7665722e20476f6c64206d696e696e67206973206f6e65206f6620746865206d6f737420646573747275637469766520696e647573747269657320696e2074686520776f726c642e2049742063616e20646973706c61636520636f6d6d756e69746965732c20636f6e74616d696e617465206472696e6b696e672077617465722c206875727420776f726b6572732c20616e642064657374726f79207072697374696e6520656e7669726f6e6d656e74732e20497420706f6c6c7574657320776174657220616e64206c616e642077697468206d65726375727920616e64206379616e6964652c20656e64616e676572696e6720746865206865616c7468206f662070656f706c6520616e642065636f73797374656d732e0a4f757220556e6465727374616e64696e67206f66206120547261646974696f6e616c20417373657420486173204368616e6765642e0a416e20617373657420697320616e797468696e67206f662076616c7565206f722061207265736f75726365206f662076616c756520746861742063616e20626520636f6e76657274656420696e746f20636173682e20536f20616e206173736574e28099732076616c75652077696c6c20646570656e64206f6e2074686520636f6c6c6563746976652062656c69656620616e64207472757374206f66207468652070656f706c65206465616c696e6720776974682069742e200a5468652067726f77746820696e20746865206e756d626572206f662063727970746f63757272656e63696573206973206368616e67696e6720616c6c206f6620746869732c20616e642074686520666169746820706c6163656420696e207468656d20627920696e766573746f72732069732064726976696e6720636f6e666964656e636520696e207468656d20617320616e20617373657420636c6173732e20496620696e766573746f727320636f6e74696e756520746f2062656c6965766520696e207468652076616c7565206f6620676f6c642062656361757365206f74686572732062656c6965766520696e2069742c2069742077696c6c2072656d61696e20616e2061737365742e2054686520646966666572656e6365206265747765656e2063727970746f63757272656e6369657320746f64617920616e6420676f6c6420696e207468652070617374206973207468657265666f7265206d696e696d616c2e0a4974206973206e6f74206120736563726574207468617420426974636f696e20697320746865206d6f73742076616c75656420616e64207468657265627920617474726163746976652063727970746f63757272656e6379206f6e20746865206d61726b65742e20457870657274732068617665206c617267656c79206372656469746564207468697320746f206974732073636172636974792e200a536361726369747920696e63726561736573207468652076616c7565206f6620616e2061737365742c207468657265666f726520746865204572676f6c64207175616e74697479206973737565642077696c6c20626520636170706564206174203535206d696c6c696f6e2e20546865206e756d626572206f66206b696c6f6772616d73206f6620676f6c64206c65667420746f20626520646973636f76657265642e200a4c65742773207265647563652074686520656e7669726f6e6d656e74616c20696d7061637420616e64206d616b652069747320707261637469636573206d6f7265207375737461696e61626c6520776974682074686520626c6f636b636861696e20746563686e6f6c6f67792e",
    "0e9a027b22373231223a7b226572676f61742d3030333436223a7b22696e646578223a3334362c2267656e65726174696f6e223a312c226261636b67726f756e64223a2250737963686f222c226261636b5f6163636573736f7279223a22477265656e20506172726f74222c22626f6479223a22507572706c65222c22636c6f74686573223a225768697465204552476f6174205368697274222c226d6f757468223a2247726974746564205465657468222c2265796573223a22416e67727920526564222c227769656c64223a225370697269742047756e222c226163636573736f7279223a2253696c766572204d6564616c222c2268656164223a22436f77626f7920486174222c2265617272696e67223a224e6f6e65227d7d7d",
    "0f00",
    "0f0102",
    "0f020000",
    "0f020202",
    "0f0204a00b",
    "0f0208cd02",
    "0f020e10",
    "0f023636",
    "0f03000000",
    "0f03043603",
    "0f03363636",
    "0f050202000000",
    "0f050804020202",
    "0f0702020000000000",
    "0f0706040602040204",
    "0f1a3200b40132b80364e807c8019811f403c8011eea0332cc08649213fa01e601009c0400b009008c1500",
    "0f200202020202020202020202020202020202020202020202020202020202020202",
    "0f60020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202",
    "0f7002020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020201010101010101010101010101010101",
    "0f7f02020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020201010101010101010101010101010101010101010101010101010101010101",
    "1000",
    "100102",
    "10020000",
    "10020202",
    "100204a00b",
    "100208cd02",
    "10020e10",
    "10023636",
    "1003000000",
    "1003043603",
    "1003363636",
    "10050202000000",
    "10050804020202",
    "100702020000000000",
    "100702020000000600",
    "100706040602040204",
    "101a3200b40132b80364e807c8019811f403c8011eea0332cc08649213fa01e601009c0400b009008c1500",
    "10200202020202020202020202020202020202020202020202020202020202020202",
    "107f02020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020201010101010101010101010101010101010101010101010101010101010101",
    "110100",
    "110102",
    "110124",
    "11018080e1eb17",
    "11018081c3b5df03",
    "11018081f0e5dd60",
    "110182d2ad9dce60",
    "12010100",
    "1202010106018271d5b481",
    "12010112",
    "12020500bebc2000050efb586040",
    "1202050efb5860400500bebc2000",
    "1201060182ee5e0040",
    "120206018271d5b48106018271d5b481",
    "1a031c4c657427732063656c656272617465204572676f526166666c6521201c4c657427732063656c656272617465204572676f526166666c65212020e730bbae0463346f8ce72be23ab8391d1e7a58f48ed857fcf4ee9aecf6915307",
    "0c0c1a00",
    "0c0c1a0101010201ff",
    "0c400504b40180febe81027880d4d4ab015a80bfdf80013c80aaea55",
    "0c4c0e01240008cd0302122c332fd4e3c901f045ac18f559dcecf8dc61f6f94fbb34d0c7c3aac71fb714",
    "0c4c0e03240008cd026d9d81d27185efa93c148f700839183a882aae3a4de1f984faff69eeed37202706240008cd026dd353119c75189796b3fb01c60289399f5fa2e7e115f4d8e3ffcc0a4ba5326906240008cd0287352ce40ff53154c5b3751a661908d3ca99edbb198e7ebb63d1d00e580f2efd06",
    "0c4c0e01240008cd0315a5d99a010bf189b1abae2d9f21be6f3438803aca1e6aac739fbee31150d62700",
    "0c3c0e0e02240008cd03f2d7187f56156cbedde84dffd873f59db7c0e16408c475145a0415317d85cf573339694a6b696558536f6f6b4c74615972384a5a3841386e4b75657639647a524d77786b476a75795165626e5167436a387a6443240008cd02d481d399b808586e94dfd907439b2671999e1d7a97b1705d3363707930a6ec59333967386569796970477666557a675239586a6761423577597641426f447a535969716a754a6b39676769446b334a533476454a",
    "0c0c580202020406080208060402",
    "580404",
    "40050002",
    "40060205029d6f084b",
    "40020e01",
    "400e02020a0c",
    "3c0e0e0350525011596f7572206c6f616e204a616e75617279",
    "480e0102020a0c0102",
    "4f0e208743542e50d2195907ce017595f8adf1f496c796d9bcc1148ff9ec94d0bf5006036ebe10da76e99b081b5893635db7518a062bd0f89b07fc056ad9b77c2abce607",
    "3c0c3c0e0e3c0c3c0e580c3c0e58010301020303040506010301020314160103040506181a",
    "3c0c3c0e0e3c0c3c0e580c3c0e58060a6261636b67726f756e6404626c75650570756e6b730361706505626561726409626967206265617264056d6f757468066d6f6465737407676c617373657302767203746f700a7065616b207370696b650000",
    "643100d2e101ff01fc047c7f6f00ff80129df69a5090012f01ffca99f5bfff0c803601800100",
    "643100d2e101ff01fc047c7f6f00ff80129df69a5090012f01ffca99f5bfff0c80360180010115",
];

/// The miner fee contract, split by hand: header 0x10, five constants, then the template.
const FEE_CONTRACT: &str = "1005040004000e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a701730073011001020402d19683030193a38cc7b2a57300000193c2b2a57301007473027303830108cdeeac93b1a57304";
const FEE_CONSTANTS: &[&str] = &[
    "0400",
    "0400",
    "0e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a70173007301",
    "100102",
    "0402",
];
const FEE_TEMPLATE: &str =
    "d19683030193a38cc7b2a57300000193c2b2a57301007473027303830108cdeeac93b1a57304";

/// A template that reads as no constant: a `SigmaPropConstant(TrueProp)` body.
const TEMPLATE: &[u8] = &[0x08, 0xd3];

fn h(s: &str) -> Vec<u8> {
    hex::decode(s).unwrap()
}

/// A segregated tree, without a size, holding `constants` before `template`.
fn segregated(constants: &[&[u8]], template: &[u8]) -> Vec<u8> {
    let mut tree = vec![CONSTANT_SEGREGATION_FLAG, u8::try_from(constants.len()).unwrap()];
    constants.iter().for_each(|c| tree.extend_from_slice(c));
    tree.extend_from_slice(template);
    tree
}

/// A segregated tree with `n` constants, the count written as a VLQ.
fn tree_of_many(constants: &[Vec<u8>]) -> Vec<u8> {
    let mut tree = vec![CONSTANT_SEGREGATION_FLAG];
    let mut n = constants.len();
    while n >= 0x80 {
        tree.push((n as u8 & 0x7f) | 0x80);
        n >>= 7;
    }
    tree.push(n as u8);
    constants.iter().for_each(|c| tree.extend_from_slice(c));
    tree.extend_from_slice(TEMPLATE);
    tree
}

#[test]
fn reads_each_fleet_constant_exactly() {
    for vector in FLEET_CONSTANTS {
        let constant = h(vector);
        let tree = segregated(&[&constant], TEMPLATE);
        assert_eq!(constants(&tree), Some(vec![&constant[..]]), "{vector}");
        assert_eq!(template(&tree), Some(TEMPLATE), "{vector}");
    }
}

#[test]
fn reads_fleet_constants_in_sequence() {
    let all: Vec<Vec<u8>> = FLEET_CONSTANTS.iter().map(|v| h(v)).collect();
    let tree = tree_of_many(&all);
    let read = constants(&tree).unwrap();
    assert_eq!(read, all.iter().map(Vec::as_slice).collect::<Vec<_>>());
    assert_eq!(template(&tree), Some(TEMPLATE));
}

/// Constants are self-delimiting, so a cut-short one never reads as a constant: the parser
/// neither stops early nor reads past what the type says.
#[test]
fn fails_on_every_truncated_fleet_constant() {
    for vector in FLEET_CONSTANTS {
        let constant = h(vector);
        for len in 0..constant.len() {
            let tree = segregated(&[&constant[..len]], &[]);
            assert_eq!(constants(&tree), None, "{vector} cut to {len} bytes");
        }
    }
}

#[test]
fn splits_the_fee_contract() {
    let tree = h(FEE_CONTRACT);
    let expected: Vec<Vec<u8>> = FEE_CONSTANTS.iter().map(|c| h(c)).collect();
    assert_eq!(constants(&tree).unwrap(), expected.iter().map(Vec::as_slice).collect::<Vec<_>>());
    assert_eq!(template(&tree), Some(&h(FEE_TEMPLATE)[..]));
    let hash: [u8; 32] = Blake2b256::digest(h(FEE_TEMPLATE)).into();
    assert_eq!(template_hash(&tree), Some(crate::types::Digest(hash)));
}

/// The template sigma-state expects of a tree (sigma-state 6.0.7,
/// `ErgoTreeSpecification`, property "ErgoTree.template"): the node's own rule, not a split
/// by hand.
#[test]
fn takes_the_template_sigma_state_takes() {
    let tree = h(concat!(
        "100604000e000400040005000500d803d601e30004d602e4c6a70408d603e4c6a7050595e67201d804d604",
        "b2a5e4720100d605b2db63087204730000d606db6308a7d60799c1a7c17204d1968302019683050193c272",
        "04c2a7938c720501730193e4c672040408720293e4c672040505720393e4c67204060ec5a796830201929c",
        "998c7205029591b1720673028cb272067303000273047203720792720773057202",
    ));
    let expected = h(concat!(
        "d803d601e30004d602e4c6a70408d603e4c6a7050595e67201d804d604b2a5e4720100d605b2db63087204",
        "730000d606db6308a7d60799c1a7c17204d1968302019683050193c27204c2a7938c720501730193e4c672",
        "040408720293e4c672040505720393e4c67204060ec5a796830201929c998c7205029591b1720673028cb2",
        "72067303000273047203720792720773057202",
    ));
    assert_eq!(template(&tree), Some(&expected[..]));
    let expected = ["0400", "0e00", "0400", "0400", "0500", "0500"].map(h);
    assert_eq!(constants(&tree).unwrap(), expected.iter().map(Vec::as_slice).collect::<Vec<_>>());
}

#[test]
fn skips_the_size() {
    // Version 1 with a size: 0x18, then the size as a VLQ past one byte.
    let mut tree = vec![0x18, 0x81, 0x01, 0x01];
    tree.extend_from_slice(&h("0400"));
    tree.extend_from_slice(TEMPLATE);
    assert_eq!(constants(&tree), Some(vec![&h("0400")[..]]));
    assert_eq!(template(&tree), Some(TEMPLATE));
}

#[test]
fn reads_every_segregated_tree_in_the_fixtures() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let mut trees = std::collections::BTreeSet::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "json") {
            let name = path.file_stem().unwrap().to_str().unwrap();
            let json: serde_json::Value = fixtures::load(name);
            collect_trees(&json, &mut trees);
        }
    }
    let segregated: Vec<_> = trees
        .iter()
        .map(|t| h(t))
        .filter(|t| t[0] & CONSTANT_SEGREGATION_FLAG != 0)
        .collect();
    assert!(segregated.len() >= 10, "{} segregated trees", segregated.len());
    for tree in &segregated {
        assert!(template(tree).is_some(), "{}", hex::encode(tree));
    }
}

fn collect_trees(json: &serde_json::Value, trees: &mut std::collections::BTreeSet<String>) {
    match json {
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                match value.as_str() {
                    Some(tree) if key == "ergoTree" => {
                        trees.insert(tree.to_owned());
                    }
                    _ => collect_trees(value, trees),
                }
            }
        }
        serde_json::Value::Array(items) => items.iter().for_each(|v| collect_trees(v, trees)),
        _ => {}
    }
}

#[test]
fn does_not_read_trees_without_segregated_constants() {
    let p2pk = h("0008cd0278011ec0cf5feb92d61adb51dcb75876627ace6fd9446ab4cabc5313ab7b39a7");
    assert_eq!(template(&p2pk), None);
    assert_eq!(constants(&p2pk), None);
    assert_eq!(template_hash(&p2pk), None);
    assert_eq!(template(&[]), None);
}

#[test]
fn reads_a_tree_without_constants() {
    let tree = segregated(&[], TEMPLATE);
    assert_eq!(constants(&tree), Some(vec![]));
    assert_eq!(template(&tree), Some(TEMPLATE));
}

/// Values Fleet has no vectors for, laid out by hand from sigma-state 6.0.7's serializers.
#[test]
fn reads_values_beyond_the_fleet_vectors() {
    let ge = "02".to_owned() + &"11".repeat(32);
    let cases = [
        // UnsignedBigInt 255.
        "0901ff".to_owned(),
        // Option[Int]: none, then some(1).
        "2800".to_owned(),
        "280102".to_owned(),
        // Option[Coll[Byte]] some([0xab]).
        "320101ab".to_owned(),
        // Option[(Int, Long)] some((0, 1)): an option of a non-embeddable type.
        "244005".to_owned() + "01" + "00" + "02",
        // Coll[Boolean] and Coll[Byte] with the element type in a byte of its own.
        "0c010305".to_owned(),
        "0c0202abcd".to_owned(),
        // Coll[Coll[Byte]] [[0x01], []].
        "1a02010100".to_owned(),
        // Coll[Coll[(Int, Int)]] [[(1, 2)]]: nested collections of a non-embeddable type.
        "1858".to_owned() + "01" + "01" + "0204",
        // (Byte, Byte, Byte, Byte, Byte), by the generic tuple code.
        "6005".to_owned() + "0202020202" + "0102030405",
        // SigmaProps: true, false, a DH tuple, and atLeast(1, [dlog, and(true, false)]).
        "08d3".to_owned(),
        "08d2".to_owned(),
        "08ce".to_owned() + &ge.repeat(4),
        "0898".to_owned() + "01" + "02" + "cd" + &ge + "96" + "02" + "d3" + "d2",
        // Unit.
        "62".to_owned(),
        // (Unit, Int): a tuple with a zero-width item.
        "4c62".to_owned() + "02",
    ];
    for case in &cases {
        let constant = h(case);
        let tree = segregated(&[&constant], TEMPLATE);
        assert_eq!(constants(&tree), Some(vec![&constant[..]]), "{case}");
        for len in 0..constant.len() {
            let tree = segregated(&[&constant[..len]], &[]);
            assert_eq!(constants(&tree), None, "{case} cut to {len} bytes");
        }
    }
}

#[test]
fn does_not_read_types_a_tree_cannot_segregate() {
    for case in [
        "63",   // Box
        "68",   // Header
        "6600", // String
        "61",   // Any
        "69",   // PreHeader
        "6a",   // Global
        "65",   // Context
        "0a00", // primitive code 10
        "0b00", // primitive code 11
        "1600", // Coll of primitive code 10
        "00",   // no type
    ] {
        let tree = segregated(&[&h(case)], TEMPLATE);
        assert_eq!(constants(&tree), None, "{case}");
    }
}

#[test]
fn does_not_spin_on_collections_of_zero_width_values() {
    // Coll[Unit] of 65,535 units, and Coll[(Unit, Unit)] of 3.
    for case in ["0c62ffff03", "0c3c626203"] {
        let tree = segregated(&[&h(case)], TEMPLATE);
        assert_eq!(constants(&tree), None, "{case}");
    }
}

#[test]
fn bounds_nesting() {
    // Coll[Coll[...Coll[Int]...]] nested past the bound, and an and(and(...)) proposition.
    let mut deep_type = vec![0x0c; MAX_DEPTH + 2];
    deep_type.extend_from_slice(&[0x04, 0x00]);
    let mut deep_sigma = vec![SIGMA_PROP];
    for _ in 0..=MAX_DEPTH + 1 {
        deep_sigma.extend_from_slice(&[AND, 0x01]);
    }
    deep_sigma.push(TRUE_PROP);
    for constant in [deep_type, deep_sigma] {
        assert_eq!(constants(&segregated(&[&constant], TEMPLATE)), None);
    }
}

#[test]
fn fails_on_a_count_past_the_constants() {
    let mut tree = segregated(&[&h("0400")], &[]);
    tree[1] = 2;
    assert_eq!(constants(&tree), None);
}

/// A register's byte collection, in either type encoding, and a pair of them, as a pair or a
/// 2-tuple.
#[test]
fn reads_byte_collections_and_pairs_of_them() {
    assert_eq!(byte_coll(&h("0e03010203")), Some(&[1, 2, 3][..]));
    assert_eq!(byte_coll(&h("0c0203010203")), Some(&[1, 2, 3][..]));
    assert_eq!(byte_coll(&h("0e00")), Some(&[][..]));
    let pair = Some((&[1][..], &[2, 3][..]));
    for constant in ["3c0e0e0101020203", "60020e0e0101020203", "3c0c020c020101020203"] {
        assert_eq!(byte_coll_pair(&h(constant)), pair, "{constant}");
        assert_eq!(byte_coll(&h(constant)), None, "{constant}");
    }
}

/// Any other type, a value cut short, a length past the end, or bytes past the value is none.
#[test]
fn reads_no_other_constant_as_byte_collections() {
    for constant in [
        "0402",
        "0c0103",
        "1a0100",
        "0e0301020300",
        "0e030102",
        "0e80",
        "0effffffffffffffffffff01",
        "3c0e040101",
        "3c0e0e010102020300",
        "60030e0e0e010101010101",
    ] {
        assert_eq!(byte_coll(&h(constant)), None, "{constant}");
        assert_eq!(byte_coll_pair(&h(constant)), None, "{constant}");
    }
    for constant in FLEET_CONSTANTS.iter().filter(|c| !c.starts_with("0e")) {
        assert_eq!(byte_coll(&h(constant)), None, "{constant}");
    }
}

/// No prefix of a recorded minting box's registers reads, and none panics.
#[test]
fn reads_no_truncated_register() {
    let minting: crate::types::ergo::BoxById = fixtures::load("minting_box_picture");
    let registers = minting.indexed.utxo.registers;
    for r in [&registers.r7, &registers.r8, &registers.r9] {
        let bytes = &r.as_ref().unwrap().0;
        assert!(byte_coll(bytes).is_some() || byte_coll_pair(bytes).is_some());
        for end in 0..bytes.len() {
            let cut = &bytes[..end];
            assert_eq!(byte_coll(cut), None, "{}", hex::encode(cut));
            assert_eq!(byte_coll_pair(cut), None, "{}", hex::encode(cut));
        }
    }
}

#[test]
fn parses_a_template_from_hex_in_either_case_within_the_box_size() {
    let template = parse_template(FEE_TEMPLATE).unwrap();
    assert_eq!(template.0[..], h(FEE_TEMPLATE)[..]);
    assert_eq!(parse_template(&FEE_TEMPLATE.to_uppercase()), Ok(template));
    let longest = "ab".repeat(MAX_TEMPLATE_BYTES);
    assert_eq!(parse_template(&longest).unwrap().0.len(), MAX_TEMPLATE_BYTES);
    for bad in ["", "a", "zz", &format!("{longest}ab")] {
        assert!(parse_template(bad).is_err(), "{}", &bad[..bad.len().min(8)]);
    }
}
