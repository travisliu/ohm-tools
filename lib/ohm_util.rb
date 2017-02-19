# encoding: UTF-8

module OhmUtil
  LUA_CACHE   = Hash.new { |h, k| h[k] = Hash.new }
  LUA_SAVE    = File.expand_path("../lua/save.lua",   __FILE__)
  LUA_DELETE  = File.expand_path("../lua/delete.lua", __FILE__)

  # All of the known errors in Ohm can be traced back to one of these
  # exceptions.
  #
  # MissingID:
  #
  #   Comment.new.id # => nil
  #   Comment.new.key # => Error
  #
  #   Solution: you need to save your model first.
  #
  # IndexNotFound:
  #
  #   Comment.find(:foo => "Bar") # => Error
  #
  #   Solution: add an index with `Comment.index :foo`.
  #
  # UniqueIndexViolation:
  #
  #   Raised when trying to save an object with a `unique` index for
  #   which the value already exists.
  #
  #   Solution: rescue `Ohm::UniqueIndexViolation` during save, but
  #   also, do some validations even before attempting to save.
  #
  class Error < StandardError; end
  class MissingID < Error; end
  class IndexNotFound < Error; end
  class UniqueIndexViolation < Error; end

  module ErrorPatterns
    DUPLICATE = /(UniqueIndexViolation: (\w+))/.freeze
    NOSCRIPT = /^NOSCRIPT/.freeze
  end

  # Used by: `attribute`, `counter`, `set`, `reference`,
  # `collection`.
  #
  # Employed as a solution to avoid `NameError` problems when trying
  # to load models referring to other models not yet loaded.
  #
  # Example:
  #
	#   class Comment < Ohm::Model
	#     reference :user, User # NameError undefined constant User.
	#   end
	#
	#   # Instead of relying on some clever `const_missing` hack, we can
	#   # simply use a symbol or a string.
	#
	#   class Comment < Ohm::Model
	#     reference :user, :User
	#     reference :post, "Post"
	#   end
	#
	def self.const(context, name)
		case name
		when Symbol, String
			context.const_get(name)
		else name
		end
	end

	def self.dict(arr)
		Hash[*arr]
	end

	def self.sort_options(options)
		args = []

		args.concat(["BY", options[:by]]) if options[:by]
		args.concat(["GET", options[:get]]) if options[:get]
		args.concat(["LIMIT"] + options[:limit]) if options[:limit]
		args.concat(options[:order].split(" ")) if options[:order]
		args.concat(["STORE", options[:store]]) if options[:store]

		return args
	end

	# Run lua scripts and cache the sha in order to improve
	# successive calls.
	def script(redis, file, *args)
		begin
			cache = LUA_CACHE[redis.url]

			if cache.key?(file)
				sha = cache[file]
			else
				src = File.read(file)
				sha = redis.call("SCRIPT", "LOAD", src)

				cache[file] = sha
			end

			redis.call!("EVALSHA", sha, *args)

		rescue RuntimeError

			case $!.message
			when ErrorPatterns::NOSCRIPT
				LUA_CACHE[redis.url].clear
				retry
			when ErrorPatterns::DUPLICATE
				raise UniqueIndexViolation, $1
			else
				raise $!
			end
		end
	end
end
