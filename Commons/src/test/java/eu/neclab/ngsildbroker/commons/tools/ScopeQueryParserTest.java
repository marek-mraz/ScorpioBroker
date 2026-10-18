package eu.neclab.ngsildbroker.commons.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

// Scope Query Language, ETSI GS CIM 009 clause 4.19; precedence from clause 4.9.
class ScopeQueryParserTest {

	private static final List<String[]> PUBLIC = List.<String[]>of(new String[] { "public", "transport" });
	private static final List<String[]> RESTRICTED = List.<String[]>of(new String[] { "restricted", "internal" });
	private static final List<String[]> BOTH = List.of(new String[] { "public", "transport" },
			new String[] { "restricted", "internal" });

	private static boolean match(String q, List<String[]> scopes) throws ResponseException {
		return QueryParser.parseScopeQuery(q).calculate(scopes);
	}

	private static String sql(String q) throws ResponseException {
		StringBuilder b = new StringBuilder();
		QueryParser.parseScopeQuery(q).toSql(b);
		return b.toString();
	}

	@Test
	void validFormsEvaluateWithAndBeforeOr() throws ResponseException {
		// spec grammar: or-list of parenthesised conjunctions
		String dnf = "(/restricted/#;/public/#),(/other/#;/public/#),/public/#";
		assertEquals(true, match(dnf, PUBLIC));
		assertEquals(false, match(dnf, RESTRICTED));
		assertEquals(true, match(dnf, BOTH));
		// bare conjunction as sent by the ETSI suite; ';' binds tighter than ','
		assertEquals(false, match("/restricted/#,/other/#;/public/#", PUBLIC));
		assertEquals(true, match("/restricted/#,/other/#;/public/#", RESTRICTED));
		assertEquals(true, match("/public/#|/other/#", PUBLIC));
		assertEquals(true, match("/#", RESTRICTED));
		assertEquals(true, match("/+/internal", RESTRICTED));
		assertEquals(true, match("/public/#;(/restricted/#,/public/#)", PUBLIC));
		assertEquals(false, match("(/restricted/#,/public/#);/restricted/#", PUBLIC));
	}

	@Test
	void sqlKeepsGroupsSoTheChainIsHomogeneous() throws ResponseException {
		assertEquals("(matchScope(scopes,'^\\/a$') and matchScope(scopes,'^\\/b$')) or matchScope(scopes,'^\\/c$')",
				sql("/a;/b,/c"));
	}

	@Test
	void malformedInputIsBadRequest() {
		for (String bad : new String[] { "", "/", "/public/#)", "(/public/#", "/public,", ";/a", "/pub-lic",
				"/public'x", "/a/#/b", "public", "()" }) {
			assertThrows(ResponseException.class, () -> QueryParser.parseScopeQuery(bad), bad);
		}
	}
}
