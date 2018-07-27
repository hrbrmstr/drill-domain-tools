package hrbrmstr.drill.udf;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

import crawlercommons.domains.EffectiveTldFinder;

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

      org.apache.drill.exec.expr.holders.VarCharHolder row =  new org.apache.drill.exec.expr.holders.VarCharHolder();

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
        row.start = 0; 
        row.end = hostBytes.length; 
        row.buffer = buffer;
        mw.varChar("hostname").write(row);
      }

      if (assigned != null) {
        byte[] assignBytes = assigned.getBytes();
        buffer.reallocIfNeeded(assignBytes.length); 
        buffer.setBytes(0, assignBytes);
        row.start = 0; 
        row.end = assignBytes.length; 
        row.buffer = buffer;
        mw.varChar("assigned").write(row);
      }

      if (subdomain != null) {
        byte[] subBytes = subdomain.getBytes();
        buffer.reallocIfNeeded(subBytes.length); 
        buffer.setBytes(0, subBytes);
        row.start = 0; 
        row.end = subBytes.length; 
        row.buffer = buffer;
        mw.varChar("subdomain").write(row);
      }

      if (tld != null) {
        byte[] tldBytes = tld.getBytes();
        buffer.reallocIfNeeded(tldBytes.length); 
        buffer.setBytes(0, tldBytes);
        row.start = 0; 
        row.end = tldBytes.length; 
        row.buffer = buffer;
        mw.varChar("tld").write(row);
      }

    } catch (Exception e) { 
    }

  }
  
}
