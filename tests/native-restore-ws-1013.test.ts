import { describe, expect, it } from "vitest";
import { classifyClobRemoteClose } from "@pm/market-data";

describe("native restore CLOB close diagnostics", () => {
  it("classifies websocket close code 1013 as slow_consumer", () => {
    expect(classifyClobRemoteClose({ code: 1013, wasClean: false })).toBe("slow_consumer");
  });

  it("keeps ordinary clean and abnormal close classifications unchanged", () => {
    expect(classifyClobRemoteClose({ code: 1000, wasClean: true })).toBe("clean_remote_close");
    expect(classifyClobRemoteClose({ code: 1001, wasClean: true })).toBe("clean_remote_close");
    expect(classifyClobRemoteClose({ code: 1006, wasClean: false })).toBe("abnormal_remote_close");
  });
});
