package hrbrmstr.drill.udf;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

// import crawlercommons.domains.EffectiveTldFinder;

import javax.inject.Inject;


@FunctionTemplate(
  names = { "suffix_extract" },
  scope = FunctionTemplate.FunctionScope.SIMPLE,
  nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
)
public class SuffixExtract implements DrillSimpleFunc {
  
  @Param VarCharHolder dom_string;
  
  @Output BaseWriter.ComplexWriter out;
  
  @Inject DrillBuf buffer;
    
  public void setup() {}
  
  public void eval() {
    
    org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter mw = out.rootAsMap();

    String input = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
      dom_string.start, dom_string.end, dom_string.buffer
    );
    
    try {

      String hostname = null;
      String assigned = null;
      String subdomain = null;
      String tld = null;

      input = input.toLowerCase();
      input = input.trim();

      if (input.length() > 0) {

        if (input.indexOf(" ") == -1) { // could add more invalid name checks

          crawlercommons.domains.EffectiveTldFinder.EffectiveTLD tld_i = crawlercommons.domains.EffectiveTldFinder.getEffectiveTLD(input);

          if (tld_i != null) {
    
            tld = tld_i.getDomain();
            
            assigned = crawlercommons.domains.EffectiveTldFinder.getAssignedDomain(input);
    
            String remainder = input.substring(0, input.lastIndexOf(assigned));
    
            // remove trailing "." if it exists
            if (remainder.endsWith(".")) remainder = remainder.substring(0, remainder.length()-1);
    
            if (remainder.indexOf(".") == -1) { // no subdomains
              if (remainder.length() > 0) hostname = remainder;
            } else {
              String parts[] = remainder.split("\\.", 2);
              subdomain = parts[1];
              hostname = parts[0];
            }
            
          }
    
        }

      }

      if (hostname != null) {
        byte[] hostBytes = hostname.getBytes();
        buffer.reallocIfNeeded(hostBytes.length); 
        buffer.setBytes(0, hostBytes);
        mw.varChar("hostname").writeVarChar(0, hostBytes.length, buffer);
      }

      if (assigned != null) {
        byte[] assignBytes = assigned.getBytes();
        buffer.reallocIfNeeded(assignBytes.length); 
        buffer.setBytes(0, assignBytes);
        mw.varChar("assigned").writeVarChar(0, assignBytes.length, buffer);
      }

      if (subdomain != null) {
        byte[] subBytes = subdomain.getBytes();
        buffer.reallocIfNeeded(subBytes.length); 
        buffer.setBytes(0, subBytes);
        mw.varChar("subdomain").writeVarChar(0, subBytes.length, buffer);
      }

      if (tld != null) {
        byte[] tldBytes = tld.getBytes();
        buffer.reallocIfNeeded(tldBytes.length); 
        buffer.setBytes(0, tldBytes);
        mw.varChar("tld").writeVarChar(0, tldBytes.length, buffer);
      }

    } catch (Exception e) { 
    }

  }
  
}
