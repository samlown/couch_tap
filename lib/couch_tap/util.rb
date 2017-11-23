module CouchTap
  module Util
    BOOL_MAP = {"true"=>true, true=>true, "false"=>false, false=>false}

    def self.str2bool(str_bool)
      return BOOL_MAP.fetch(str_bool, false)
    end
  end
end
