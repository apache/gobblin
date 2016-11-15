package gobblin.source.extractor.extract.google.webmaster;

import com.google.api.services.webmasters.model.ApiDimensionFilter;
import com.google.api.services.webmasters.model.ApiDimensionFilterGroup;
import java.util.HashMap;
import java.util.List;


public class GoogleWebmasterFilter {

  enum Dimension {
    DATE, PAGE, COUNTRY, QUERY, DEVICE, SEARCH_TYPE, SEARCH_APPEARANCE
  }

  enum Country {
    ALL, USA, INDIA, GERMANY
  }

  //TODO: this is case insensitive
  enum FilterOperator {
    EQUALS, CONTAINS, NOTCONTAINS
  }

  private static HashMap<Country, String> countryToCode = new HashMap<>();
  private static HashMap<String, Country> codeToCountry = new HashMap<>();

  static {
    countryToCode.put(Country.USA, "usa");
    codeToCountry.put("usa", Country.USA);

    countryToCode.put(Country.INDIA, "ind");
    codeToCountry.put("ind", Country.INDIA);

    countryToCode.put(Country.GERMANY, "deu");
    codeToCountry.put("deu", Country.GERMANY);
  }

  private static ApiDimensionFilter build(String dimension, String operator, String expression) {
    return new ApiDimensionFilter().setDimension(dimension).setOperator(operator).setExpression(expression);
  }

  static ApiDimensionFilter pageFilter(FilterOperator op, String expression) {
    //Operator string is case insensitive
    return build(Dimension.PAGE.toString(), op.toString(), expression);
  }

  static ApiDimensionFilter countryFilter(Country country) {
    if (country == Country.ALL) {
      return null;
    }
    return build(Dimension.COUNTRY.toString(), FilterOperator.EQUALS.toString().toLowerCase(),
        countryToCode.get(country));
  }

  static Country countryFilterToEnum(ApiDimensionFilter countryFilter) {
    Country country;
    if (countryFilter == null) {
      country = Country.ALL;
    } else {
      country = codeToCountry.get(countryFilter.getExpression());
    }
    return country;
  }

  static ApiDimensionFilterGroup andGroupFilters(List<ApiDimensionFilter> filters) {
    if (filters == null || filters.isEmpty()) {
      return null;
    }
    return new ApiDimensionFilterGroup().setFilters(filters).setGroupType("and");
  }

    /* All country codes:
    "usa",  USA
    "ind",  India
    "gbr",
    "can",
    "bra",
    "fra",
    "deu",  Germany
    "ita",
    "phl",
    "aus",
    "esp",
    "nld",
    "idn",
    "tur",
    "pak",
    "are",
    "zaf",
    "pol",
    "vnm",
    "bel",
    "sgp",
    "mys",
    "irn",
    "mex",
    "bgd",
    "irl",
    "che",
    "sau",
    "swe",
    "rou",
    "egy",
    "grc",
    "hkg",
    "nga",
    "dnk",
    "jpn",
    "arg",
    "ken",
    "prt",
    "ukr",
    "fin",
    "rus",
    "srb",
    "col",
    "per",
    "mar",
    "isr",
    "tha",
    "twn",
    "nor",
    "nzl",
    "hun",
    "aut",
    "chl",
    "zzz",
    "kor",
    "hrv",
    "cze",
    "dza",
    "qat",
    "bgr",
    "lka",
    "tun",
    "chn",
    "gha",
    "jor",
    "svk",
    "ven",
    "lbn",
    "npl",
    "kwt",
    "ltu",
    "pri",
    "svn",
    "irq",
    "omn",
    "tza",
    "uga",
    "ecu",
    "cri",
    "lux",
    "cyp",
    "bih",
    "jam",
    "mkd",
    "bhr",
    "aze",
    "lva",
    "alb",
    "dom",
    "zwe",
    "mus",
    "pan",
    "blr",
    "est",
    "mlt",
    "tto",
    "eth",
    "blz",
    "gtm",
    "kaz",
    "ury",
    "geo",
    "zmb",
    "xkk",
    "khm",
    "mmr",
    "slv",
    "sdn",
    "arm",
    "civ",
    "cmr",
    "bol",
    "syr",
    "afg",
    "ago",
    "nic",
    "mda",
    "moz",
    "yem",
    "pse",
    "sen",
    "hnd",
    "brb",
    "nam",
    "lby",
    "bwa",
    "mdv",
    "rwa",
    "isl",
    "mne",
    "brn",
    "som",
    "pry",
    "bhs",
    "mng",
    "reu",
    "mdg",
    "cod",
    "png",
    "jey",
    "mwi",
    "mac",
    "cym",
    "uzb",
    "bmu",
    "hti",
    "fji",
    "ben",
    "imn",
    "gum",
    "mli",
    "ggy",
    "cuw",
    "lao",
    "lso",
    "cub",
    "vir",
    "lbr",
    "sur",
    "guy",
    "sle",
    "gib",
    "kgz",
    "abw",
    "bfa",
    "swz",
    "btn",
    "tgo",
    "lca",
    "glp",
    "grd",
    "mtq",
    "gin",
    "cog",
    "mrt",
    "atg",
    "gab",
    "tkm",
    "gmb",
    "and",
    "mco",
    "ssd",
    "vct",
    "bdi",
    "ner",
    "dji",
    "ncl",
    "pyf",
    "sxm",
    "tjk",
    "cpv",
    "syc",
    "kna",
    "lie",
    "dma",
    "tcd",
    "tca",
    "vgb",
    "tls",
    "maf",
    "mnp",
    "ala",
    "fro",
    "aia",
    "guf",
    "slb",
    "smr",
    "grl",
    "myt",
    "gnq",
    "wsm",
    "caf",
    "bes",
    "cok",
    "asm",
    "vut",
    "prk",
    "ton",
    "eri",
    "blm",
    "fsm",
    "gnb",
    "kir",
    "plw",
    "com",
    "ata",
    "stp",
    "mhl",
    "iot",
    "msr",
    "flk",
    "sjm",
    "nfk",
    "shn",
    "spm",
    "nru",
    "cxr",
    "esh",
    "tuv",
    "wlf",
    "niu",
    "umi"
     */
}
