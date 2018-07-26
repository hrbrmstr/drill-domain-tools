package hrbrmstr.drill.udf;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

import crawlercommons.domains.EffectiveTldFinder;

import javax.inject.Inject;


@FunctionTemplate(
  names = { "suffix_extract" },
  scope = FunctionTemplate.FunctionScope.SIMPLE,
  nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
)
public class SuffixExtract implements DrillSimpleFunc {
  
  @Param NullableVarCharHolder dom_string;
  
  @Output BaseWriter.ComplexWriter out;
  
  @Inject DrillBuf buffer;
    
  public void setup() {}
  
  public void eval() {
    
    org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter mw = out.rootAsMap();

    String input = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
      dom_string.start, dom_string.end, dom_string.buffer
    );
    
    if (input.isEmpty() || input == null) input = "";

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

    org.apache.drill.exec.expr.holders.VarCharHolder row;
    byte[] outBytes;

    if (hostname != null) {
      row = new org.apache.drill.exec.expr.holders.VarCharHolder();
      outBytes = hostname.getBytes();
      buffer.reallocIfNeeded(outBytes.length); 
      buffer.setBytes(0, outBytes);
      row.start = 0; 
      row.end = outBytes.length; 
      row.buffer = buffer;
      mw.varChar("hostname").write(row);
    }

    if (assigned != null) {
      row = new org.apache.drill.exec.expr.holders.VarCharHolder();
      outBytes = assigned.getBytes();
      buffer.reallocIfNeeded(outBytes.length); 
      buffer.setBytes(0, outBytes);
      row.start = 0; 
      row.end = outBytes.length; 
      row.buffer = buffer;
      mw.varChar("assigned").write(row);
    }

    if (subdomain != null) {
      row = new org.apache.drill.exec.expr.holders.VarCharHolder();
      outBytes = subdomain.getBytes();
      buffer.reallocIfNeeded(outBytes.length); 
      buffer.setBytes(0, outBytes);
      row.start = 0; 
      row.end = outBytes.length; 
      row.buffer = buffer;
      mw.varChar("subdomain").write(row);
    }

    if (tld != null) {
      row = new org.apache.drill.exec.expr.holders.VarCharHolder();
      outBytes = tld.getBytes();
      buffer.reallocIfNeeded(outBytes.length); 
      buffer.setBytes(0, outBytes);
      row.start = 0; 
      row.end = outBytes.length; 
      row.buffer = buffer;
      mw.varChar("tld").write(row);
    }
    
  }
  
}
